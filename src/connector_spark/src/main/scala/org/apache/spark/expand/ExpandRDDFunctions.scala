/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.expand

import java.nio.ByteBuffer
import java.util.{HashMap => JHashMap}

import scala.collection.{mutable, Map}
import scala.collection.mutable.ArrayBuffer
// import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.mapred.{FileOutputCommitter, FileOutputFormat, JobConf, OutputFormat}
import org.apache.hadoop.mapreduce.{Job => NewAPIHadoopJob, OutputFormat => NewOutputFormat}

import org.apache.spark._
import org.apache.spark.Partitioner.defaultPartitioner
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.SPECULATION_ENABLED
import org.apache.spark.internal.io._
import org.apache.spark.partial.{BoundedDouble, PartialResult}
import org.apache.spark.serializer.Serializer
import org.apache.spark.util.{SerializableConfiguration, SerializableJobConf, Utils}
// import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.collection.CompactBuffer
import org.apache.spark.util.random.StratifiedSamplingUtils
import org.apache.spark.rdd._
import org.apache.spark.api.java.JavaRDD

import org.expand.spark.ExpandOutputCommitter

/**
 * Extra functions available on RDDs of (key, value) pairs through an implicit conversion.
 */
class ExpandRDDFunctions[K, V](self: JavaRDD[(K, V)])
    (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null)
  extends Logging with Serializable {

  /**
   * Output the RDD to any Hadoop-supported file system, using a Hadoop `OutputFormat` class
   * supporting the key and value types K and V in this RDD.
   */
  def saveAsExpandFile[F <: OutputFormat[K, V]](
      path: String)(implicit fm: ClassTag[F]): Unit = self.withScope {
    saveAsExpandFile(path, keyClass, valueClass, fm.runtimeClass.asInstanceOf[Class[F]])
  }

  /**
   * Output the RDD to any Hadoop-supported file system, using a Hadoop `OutputFormat` class
   * supporting the key and value types K and V in this RDD. Compress the result with the
   * supplied codec.
   */
  def saveAsExpandFile[F <: OutputFormat[K, V]](
      path: String,
      codec: Class[_ <: CompressionCodec])(implicit fm: ClassTag[F]): Unit = self.withScope {
    val runtimeClass = fm.runtimeClass
    saveAsExpandFile(path, keyClass, valueClass, runtimeClass.asInstanceOf[Class[F]], codec)
  }

  /**
   * Output the RDD to any Hadoop-supported file system, using a Hadoop `OutputFormat` class
   * supporting the key and value types K and V in this RDD. Compress with the supplied codec.
   */
  def saveAsExpandFile(
      path: String,
      keyClass: Class[_],
      valueClass: Class[_],
      outputFormatClass: Class[_ <: OutputFormat[_, _]],
      codec: Class[_ <: CompressionCodec]): Unit = self.withScope {
    saveAsExpandFile(path, keyClass, valueClass, outputFormatClass,
      new JobConf(self.context.hadoopConfiguration), Option(codec))
  }

  /**
   * Output the RDD to any Hadoop-supported file system, using a Hadoop `OutputFormat` class
   * supporting the key and value types K and V in this RDD.
   *
   * @note We should make sure our tasks are idempotent when speculation is enabled, i.e. do
   * not use output committer that writes data directly.
   * There is an example in https://issues.apache.org/jira/browse/SPARK-10063 to show the bad
   * result of using direct output committer with speculation enabled.
   */
  def saveAsExpandFile(
      path: String,
      keyClass: Class[_],
      valueClass: Class[_],
      outputFormatClass: Class[_ <: OutputFormat[_, _]],
      conf: JobConf = new JobConf(self.context.hadoopConfiguration),
      codec: Option[Class[_ <: CompressionCodec]] = None): Unit = self.withScope {
    // Rename this as hadoopConf internally to avoid shadowing (see SPARK-2038).
    val hadoopConf = conf
    hadoopConf.setOutputKeyClass(keyClass)
    hadoopConf.setOutputValueClass(valueClass)
    conf.setOutputFormat(outputFormatClass)
    for (c <- codec) {
      hadoopConf.setCompressMapOutput(true)
      hadoopConf.set("mapreduce.output.fileoutputformat.compress", "true")
      hadoopConf.setMapOutputCompressorClass(c)
      hadoopConf.set("mapreduce.output.fileoutputformat.compress.codec", c.getCanonicalName)
      hadoopConf.set("mapreduce.output.fileoutputformat.compress.type",
        CompressionType.BLOCK.toString)
    }

    // Use configured output committer if already set
    if (conf.getOutputCommitter == null) {
      hadoopConf.setOutputCommitter(classOf[ExpandOutputCommitter])
    }

    // When speculation is on and output committer class name contains "Direct", we should warn
    // users that they may loss data if they are using a direct output committer.
    val speculationEnabled = self.conf.get(SPECULATION_ENABLED)
    val outputCommitterClass = hadoopConf.get("mapred.output.committer.class", "")
    if (speculationEnabled && outputCommitterClass.contains("Direct")) {
      val warningMessage =
        s"$outputCommitterClass may be an output committer that writes data directly to " +
          "the final location. Because speculation is enabled, this output committer may " +
          "cause data loss (see the case in SPARK-10063). If possible, please use an output " +
          "committer that does not have this behavior (e.g. FileOutputCommitter)."
      logWarning(warningMessage)
    }

    FileOutputFormat.setOutputPath(hadoopConf,
      SparkHadoopWriterUtils.createPathFromString(path, hadoopConf))
    saveAsExpandDataset(hadoopConf)
  }

  /**
   * Output the RDD to any Hadoop-supported storage system, using a Hadoop JobConf object for
   * that storage system. The JobConf should set an OutputFormat and any output paths required
   * (e.g. a table name to write to) in the same way as it would be configured for a Hadoop
   * MapReduce job.
   */
  def saveAsExpandDataset(conf: JobConf): Unit = self.withScope {
    val config = new HadoopMapRedWriteConfigUtil[K, V](new SerializableJobConf(conf))
    SparkExpandWriter.write(
      rdd = self,
      config = config)
  }

  private[spark] def keyClass: Class[_] = kt.runtimeClass

  private[spark] def valueClass: Class[_] = vt.runtimeClass

  private[spark] def keyOrdering: Option[Ordering[K]] = Option(ord)

}
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

package com.github.ehiggs.spark.terasort

import java.util.Comparator
import com.google.common.primitives.UnsignedBytes
import org.apache.spark.{SparkConf, SparkContext}
import org.expand.spark.ExpandSparkFunctions
import org.apache.spark.expand.ExpandRDDFunctions

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.expand.hadoop.Expand

/**
 * This is a great example program to stress test Spark's shuffle mechanism.
 *
 * See http://sortbenchmark.org/
 */
object TeraSortXPN {

  implicit val caseInsensitiveOrdering : Comparator[Array[Byte]] =
    UnsignedBytes.lexicographicalComparator

  def main(args: Array[String]) {

    if (args.length < 2) {
      println("Usage:")
      println("DRIVER_MEMORY=[mem] spark-submit " +
        "com.github.ehiggs.spark.terasort.TeraSort " +
        "spark-terasort-1.0-SNAPSHOT-with-dependencies.jar " +
        "[input-file] [output-file]")
      println(" ")
      println("Example:")
      println("DRIVER_MEMORY=50g spark-submit " +
        "com.github.ehiggs.spark.terasort.TeraSort " +
        "spark-terasort-1.0-SNAPSHOT-with-dependencies.jar " +
        "/home/myuser/terasort_in /home/myuser/terasort_out")
      System.exit(0)
    }

    // Process command line arguments
    val inputFile = args(0)
    val outputFile = args(1)

    val conf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.hadoop.fs.defaultFS", "xpn:///")
      .set("spark.hadoop.fs.xpn.impl", "org.expand.hadoop.Expand")
      .setAppName(s"TeraSort")
    val sc = new SparkContext(conf)
    sc.setLocalProperty("spark.scheduler.pool", "pool1")

    val dataset = sc.newAPIHadoopFile[Array[Byte], Array[Byte], TeraInputFormatXPN](inputFile)
    val startTime = System.currentTimeMillis()
    val sorted = dataset.repartitionAndSortWithinPartitions(
      new TeraSortPartitioner(dataset.partitions.length))
    // sorted.saveAsNewAPIHadoopFile[TeraOutputFormat](outputFile)
    ExpandSparkFunctions.writeExpand(sorted, outputFile, sc.hadoopConfiguration);
    // sorted.saveAsNewAPIHadoopFile[TeraOutputFormat](outputFile)
    val endTime = System.currentTimeMillis()
    println(s"==== TeraSort took ${(endTime-startTime)/1000.0}s ====")
    sc.stop()
  }
}

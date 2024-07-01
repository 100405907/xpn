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

import scala.collection.JavaConversions._

import java.io.EOFException
import java.util.Comparator
import java.util
import java.net.URI
import scala.collection.mutable.ListBuffer

import com.google.common.primitives.UnsignedBytes
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.JobContext
import org.apache.hadoop.mapred.JobContextImpl
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapred.JobConf
import org.expand.hadoop.Expand

object TeraInputFormatXPN {
   val KEY_LEN = 10
   val VALUE_LEN = 90
   val RECORD_LEN : Int = KEY_LEN + VALUE_LEN
   var lastContext : JobContext = _
   var lastResult : java.util.List[InputSplit] = _
   implicit val caseInsensitiveOrdering : Comparator[Array[Byte]] =
     UnsignedBytes.lexicographicalComparator
}

class TeraInputFormatXPN extends FileInputFormat[Array[Byte], Array[Byte]] {

  override def createRecordReader(split: InputSplit, context: TaskAttemptContext)
  : RecordReader[Array[Byte], Array[Byte]] = new TeraRecordReader()

  // Sort the file pieces since order matters.
  override def listStatus(job: JobContext): java.util.List[FileStatus] = {

    val conf: Configuration = new Configuration()
    conf.set("spark.hadoop.fs.defaultFS", "xpn:///")
    conf.set("spark.hadoop.fs.xpn.impl", "org.expand.hadoop.Expand")

    // val jobConf: JobConf = new JobConf(conf)

    // val new_job: JobContext = new JobContextImpl(jobConf, job.getJobID())

    // val listing = super.listStatus(new_job)

    val xpn: Expand = new Expand()
    try{
      xpn.initialize(URI.create("xpn:///"), conf)
    }

    // val dirs: Array[Path] = FileInputFormat.getInputPaths(job)
    val listing: Array[FileStatus] = xpn.listStatus(new Path("xpn:///xpn/wikipedia"))

    // for (p <- dirs) {
    //   println(p.toString())
    //   val res = xpn.listStatus(p)
    //   for (r <- res){
    //     listing.add(r)
    //   }
    // }

    val filterSortedListing = listing
      .filter(_.getPath.toString.startsWith("/xpn/wikipedia/part-r"))
      .sortWith { (lhs, rhs) =>
        lhs.getPath.compareTo(rhs.getPath) < 0
    }
    val sortedListing = filterSortedListing.map { fileStatus =>
      new FileStatus(
        fileStatus.getLen,
        fileStatus.isDirectory,
        fileStatus.getReplication,
        fileStatus.getBlockSize,
        fileStatus.getModificationTime,
        new Path("xpn:///" + fileStatus.getPath.toString)
      )
    }
    sortedListing.toList
  }

  class TeraRecordReader extends RecordReader[Array[Byte], Array[Byte]] {
    private var in : FSDataInputStream = _
    private var offset: Long = 0
    private var length: Long = 0
    private val buffer: Array[Byte] = new Array[Byte](TeraInputFormatXPN.RECORD_LEN)
    private var key: Array[Byte] = _
    private var value: Array[Byte] = _

    override def nextKeyValue() : Boolean = {
      if (offset >= length) {
        return false
      }
      var read : Int = 0
      while (read < TeraInputFormatXPN.RECORD_LEN) {
        var newRead : Int = in.read(buffer, read, TeraInputFormat.RECORD_LEN - read)
        if (newRead == -1) {
          if (read == 0) false
          else throw new EOFException("read past eof")
        }
        read += newRead
      }
      if (key == null) {
        key = new Array[Byte](TeraInputFormatXPN.KEY_LEN)
      }
      if (value == null) {
        value = new Array[Byte](TeraInputFormatXPN.VALUE_LEN)
      }
      buffer.copyToArray(key, 0, TeraInputFormatXPN.KEY_LEN)
      buffer.takeRight(TeraInputFormatXPN.VALUE_LEN)
        .copyToArray(value, 0, TeraInputFormatXPN.VALUE_LEN)
      offset += TeraInputFormatXPN.RECORD_LEN
      true
    }

    override def initialize(split : InputSplit, context : TaskAttemptContext) : Unit = {
      val fileSplit = split.asInstanceOf[FileSplit]
      val p : Path = fileSplit.getPath
      val xpn: Expand = new Expand()
      val conf: Configuration = new Configuration()
      conf.set("spark.hadoop.fs.defaultFS", "xpn:///")
      conf.set("spark.hadoop.fs.xpn.impl", "org.expand.hadoop.Expand")
      try{
        xpn.initialize(URI.create("xpn:///"), conf)
      }

      // val fs : FileSystem = p.getFileSystem(conf)
      // val fs : FileSystem = FileSystem.get(conf)
      in = xpn.open(p)
      val start : Long = fileSplit.getStart
      // find the offset to start at a record boundary
      val reclen = TeraInputFormatXPN.RECORD_LEN
      offset = (reclen - (start % reclen)) % reclen
      in.seek(start + offset)
      length = fileSplit.getLength
    }

    override def close() : Unit = in.close()
    override def getCurrentKey : Array[Byte] = key
    override def getCurrentValue : Array[Byte] = value
    override def getProgress : Float = offset / length
  }

}
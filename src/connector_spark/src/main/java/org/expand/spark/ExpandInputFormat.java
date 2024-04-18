package org.expand.spark;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.nio.charset.StandardCharsets;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class ExpandInputFormat extends FileInputFormat<LongWritable, Text> {

  @Override
  public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
    // String delimiter = context.getConfiguration().get("textinputformat.record.delimiter");
    // byte[] recordDelimiterBytes = null;
    // if (null != delimiter) recordDelimiterBytes = delimiter.getBytes(StandardCharsets.UTF_8);
    return new ExpandRecordReader();
  }

  @Override
  protected boolean isSplitable(JobContext context, Path file) {
    final CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
    if (null == codec) return true; 
    return codec instanceof SplittableCompressionCodec;
  }

}
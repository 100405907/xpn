package org.expand.tests;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.expand.hadoop.Expand;

public class WordCount {
  public static void main(String [] args) throws Exception {
    Configuration conf=new Configuration();
    // conf.set("fs.defaultFS", "xpn:///");
    // conf.set("fs.xpn.impl", "org.expand.hadoop.Expand");
    // conf.set("mapreduce.job.split.metainfo.maxsize", "-1");
    // conf.set("mapred.child.java.opts", "-Xmx4096m");
    // Expand xpn = new Expand();
    // xpn.initialize(URI.create("xpn:///"), conf);
    Path input=new Path("hdfs://localhost:9000/2000-0.txt");
    Path output=new Path("hdfs://localhost:9000/wc-out");
    // xpn.delete(output, true);
    long startTime = System.nanoTime();
    Job j=new Job(conf,"wordcount");
    j.setJarByClass(WordCount.class);
    j.setMapperClass(MapForWordCount.class);
    j.setReducerClass(ReduceForWordCount.class);
    j.setOutputKeyClass(Text.class);
    j.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(j, input);
    FileOutputFormat.setOutputPath(j, output);
    j.waitForCompletion(true);
    System.out.println("---------------------------------- " + (System.nanoTime() - startTime) + " ---------------------------------");
    System.exit(0);
  }
public static class MapForWordCount extends Mapper<LongWritable, Text, Text, IntWritable>{ 
  public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
    String line = value.toString();
    String[] words=line.split(" ");
    for(String word: words ) { 
      Text outputKey = new Text(word.toUpperCase().trim());
      IntWritable outputValue = new IntWritable(1);
      con.write(outputKey, outputValue);
    }
  }
}
public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, IntWritable> {
  public void reduce(Text word, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException {
    int sum = 0;
    for(IntWritable value : values)
    {
      sum += value.get();
    }
    con.write(word, new IntWritable(sum));
  }
 }
}
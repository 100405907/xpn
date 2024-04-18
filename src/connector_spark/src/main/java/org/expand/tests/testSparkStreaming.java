package org.expand.tests;

import org.apache.spark.expand.ExpandRDDFunctions;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;
import java.util.Arrays;
import java.util.Map;
import java.util.List;
import org.apache.spark.sql.SparkSession;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import java.net.URI;
import org.apache.hadoop.fs.Path;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.*;

import org.expand.spark.ExpandOutputFormat;
import org.expand.spark.ExpandInputFormat;
import org.expand.spark.ExpandSparkFunctions;

import org.expand.hadoop.Expand;

public class testSparkStreaming {
	public static void main(String[] args) {

        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("wc")
			.set("spark.hadoop.fs.defaultFS", "xpn:///")
			.set("spark.hadoop.fs.xpn.impl", "org.expand.hadoop.Expand")
            .setMaster("spark://nodo1:7077"));

        JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(1));

		SparkSession spark = SparkSession.builder().appName("wc")
			.config("spark.hadoop.fs.defaultFS", "xpn:///")
			.config("spark.hadoop.fs.xpn.impl", "org.expand.hadoop.Expand")
			.getOrCreate();

		Configuration xpnconf = sc.hadoopConfiguration();
		
		String filePath = "xpn:///xpn/quixote";

        jssc.start();

		JavaPairDStream<LongWritable, Text> rdd = jssc.fileStream("xpn:///xpn/wc/*", LongWritable.class, Text.class, ExpandInputFormat.class);

		JavaDStream<String> lines = rdd.map(tuple -> tuple._2().toString());

		JavaDStream<String> words = lines.flatMap(s -> Arrays.asList(s.split(" |\n")).iterator());

		JavaPairDStream<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

		JavaPairDStream<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
		
		// ExpandSparkFunctions.writeExpand(counts, "xpn:///xpn/wc-quixote", xpnconf);

		jssc.stop();
	}
}

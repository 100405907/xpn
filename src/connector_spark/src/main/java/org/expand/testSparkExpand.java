package org.expand;

import org.apache.spark.expand.*;

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
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import org.expand.spark.ExpandOutputFormat;
import org.expand.spark.ExpandInputFormat;

public class testSparkExpand {
	public static void main(String[] args) {

		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("wc")
			.set("spark.hadoop.fs.defaultFS", "xpn:///")
			.set("spark.hadoop.fs.xpn.impl", "org.expand.hadoop.Expand"));

		SparkSession spark = SparkSession.builder().appName("wc")
			.config("spark.hadoop.fs.defaultFS", "xpn:///")
			.config("spark.hadoop.fs.xpn.impl", "org.expand.hadoop.Expand")
			.getOrCreate();

		Configuration xpnconf = sc.hadoopConfiguration();
		
		String filePath = "xpn:///xpn/quixote";

		JavaPairRDD<LongWritable, Text> rdd = sc.newAPIHadoopFile(
			filePath,
			ExpandInputFormat.class,
			LongWritable.class,
			Text.class,
			sc.hadoopConfiguration()
		);

		JavaRDD<String> lines = rdd.map(tuple -> tuple._2().toString());

		JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(s.split(" |\n")).iterator());

		JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

		JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
		
		JavaRDD<Tuple2<String, Integer>> finalCounts = counts.map(pair -> new Tuple2<>(pair._1(), pair._2()));

		// Path outputpath = new Path ("xpn:///xpn/wc-quixote4");
		
		xpnconf.set("xpn.output.path", "xpn:///xpn/wc-quixote");

		ExpandRDDFunctions<String, Integer> func =
                new ExpandRDDFunctions<String, Integer>(finalCounts, ClassTag$.MODULE$.apply(Text.class), ClassTag$.MODULE$.apply(IntWritable.class), null);
		
		func.saveAsExpandFile("xpn:///xpn/wc-quixote", ClassTag$.MODULE$.apply(ExpandOutputFormat.class));
		
		sc.stop();
		System.out.println("TERMINE");
	}
}

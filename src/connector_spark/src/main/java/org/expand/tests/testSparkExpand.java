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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import java.net.URI;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import org.expand.hadoop.Expand;
import org.expand.spark.ExpandOutputFormat;
import org.expand.spark.ExpandInputFormat;
import org.expand.spark.ExpandSparkFunctions;

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

		Expand xpn = new Expand();
		String filePath = "xpn:///xpn/wikipedia";
		String input = "file:///beegfs/home/javier.garciablas/gsotodos/data/wikipedia";

		try{
			xpn.initialize(URI.create("xpn:///"), xpnconf);
			xpn.loadFileToExpand(xpnconf, new Path(input), new Path(filePath));
		} catch (Exception e) {
			System.out.println("Excepcion en la carga");
		}
    	
		long startTime = System.nanoTime();

		// JavaPairRDD<LongWritable, Text> rdd = sc.newAPIHadoopFile(
		// 	filePath,
		// 	ExpandInputFormat.class,
		// 	LongWritable.class,
		// 	Text.class,
		// 	sc.hadoopConfiguration()
		// ).repartition(8);

		JavaRDD<String> rdd = sc.textFile("xpn:///xpn/wikipedia", 250);

		// JavaRDD<String> lines = rdd.map(tuple -> tuple._2().toString());

		JavaRDD<String> words = rdd.flatMap(s -> Arrays.asList(s.split(" |\n")).iterator());

		JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

		JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

		// counts.saveAsTextFile("xpn:///xpn/wc-wikipedia");
		
		ExpandSparkFunctions.writeExpand(counts, "xpn:///xpn/wc-wikipedia", xpnconf);
    	System.out.println("---------------------------------- " + (System.nanoTime() - startTime) + " ---------------------------------");

		sc.stop();
	}
}
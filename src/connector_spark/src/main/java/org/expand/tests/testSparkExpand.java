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
		String filePath1 = "xpn:///xpn/wikipedia1";
		String input1 = "file:///beegfs/home/javier.garciablas/gsotodos/data/wikipedia1";
		String filePath2 = "xpn:///xpn/wikipedia2";
		String input2 = "file:///beegfs/home/javier.garciablas/gsotodos/data/wikipedia2";
		String filePath3 = "xpn:///xpn/wikipedia3";
		String input3 = "file:///beegfs/home/javier.garciablas/gsotodos/data/wikipedia3";
		String filePath4 = "xpn:///xpn/wikipedia4";
		String input4 = "file:///beegfs/home/javier.garciablas/gsotodos/data/wikipedia4";

		try{
			xpn.initialize(URI.create("xpn:///"), xpnconf);
			xpn.loadFileToExpand(xpnconf, new Path(input1), new Path(filePath1));
			xpn.loadFileToExpand(xpnconf, new Path(input2), new Path(filePath2));
			xpn.loadFileToExpand(xpnconf, new Path(input3), new Path(filePath3));
			xpn.loadFileToExpand(xpnconf, new Path(input4), new Path(filePath4));
		} catch (Exception e) {
			System.out.println("Excepcion en la carga");
		}

		long startTime = System.nanoTime();

		JavaRDD<String> rdd = sc.textFile("xpn:///xpn/");

		JavaRDD<String> words = rdd.flatMap(s -> Arrays.asList(s.split(" |\n")).iterator());

		JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

		JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

		ExpandSparkFunctions.writeExpand(counts, "xpn:///xpn/wc-wikipedia", xpnconf);
    	System.out.println("---------------------------------- " + (System.nanoTime() - startTime) + " ---------------------------------");

		sc.stop();
	}
}
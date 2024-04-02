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
		String filePath = "xpn:///xpn/quixote";
		String input = "file:///home/resh000186/data/quixote";

		try{
			xpn.initialize(URI.create("xpn:///"), xpnconf);
			xpn.loadFileToExpand(xpnconf, new Path(input), new Path(filePath));
		} catch (Exception e) {
			System.out.println("Excepcion en la carga");
		}
    	
		long startTime = System.nanoTime();

		JavaPairRDD<Text, String> rdd = sc.newAPIHadoopFile(
			filePath,
			ExpandInputFormat.class,
			Text.class,
			String.class,
			sc.hadoopConfiguration()
		);

		JavaRDD<String> lines = rdd.map(tuple -> tuple._2());

		JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(s.split(" |\n")).iterator());

		JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

		JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
		
		ExpandSparkFunctions.writeExpand(counts, "xpn:///xpn/wc-quixote", xpnconf);
    	System.out.println("---------------------------------- " + (System.nanoTime() - startTime) + " ---------------------------------");

		try {
			xpn.delete(new Path(filePath), false);
			xpn.delete(new Path("xpn:///xpn/wc-quixote"), false);
		} catch (Exception e){
			System.out.println("Error en la limpieza");
		}

		sc.stop();
	}
}

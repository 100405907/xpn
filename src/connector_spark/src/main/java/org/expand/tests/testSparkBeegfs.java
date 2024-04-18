package org.expand.tests;

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

public class testSparkBeegfs {
	public static void main(String[] args) {

		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("wc"));

		SparkSession spark = SparkSession.builder().appName("wc")
			.getOrCreate();

		long startTime = System.nanoTime();

		JavaRDD<String> rdd = sc.textFile("/beegfs/home/javier.garciablas/gsotodos/data/wikipedia");

		JavaRDD<String> words = rdd.flatMap(s -> Arrays.asList(s.split(" |\n")).iterator());

		JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

		JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

        // JavaRDD<Tuple2<K, V>> finalrdd = counts.map(pair -> new Tuple2<>(pair._1(), pair._2()));
		
        counts.saveAsTextFile("/beegfs/home/javier.garciablas/gsotodos/data/wc-wikipedia");
		
    	System.out.println("---------------------------------- " + (System.nanoTime() - startTime) + " ---------------------------------");

		sc.stop();
	}
}
package org.expand;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;
import java.util.Arrays;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.List;
import org.apache.spark.sql.SparkSession;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FSDataInputStream;
import org.expand.Expand;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import java.net.URI;

public class testSparkExpand{
        public static void main(String[] args) {

		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("wc")
            .set("spark.hadoop.fs.defaultFS", "xpn:///")
            .set("spark.hadoop.fs.xpn.impl", "org.expand.Expand"));

		SparkSession spark = SparkSession.builder().appName("wc")
			.config("spark.hadoop.fs.defaultFS", "xpn:///")
            .config("spark.hadoop.fs.xpn.impl", "org.expand.Expand")
			.getOrCreate();
		
		Expand xpn = new Expand();

		Configuration xpnconf = sc.hadoopConfiguration();
		try{
			xpn.initialize(URI.create("xpn:///"), xpnconf);
		}catch (Exception e){
			System.out.println(e);
		}
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
		
		JavaPairRDD<Text, IntWritable> finalCounts = counts.mapToPair(pair -> new Tuple2<>(new Text(pair._1()), new IntWritable(pair._2())));

		Path outputpath = new Path ("xpn:///xpn/wc-quixote4");
		try{
			FSDataOutputStream out = xpn.create(outputpath);

			counts.foreach(tuple->{

				xpn.write(out, tuple._1(), tuple._2());

			});

			out.close();
		}catch (Exception e){
			System.out.println(e);
		}
		// xpnconf.set("xpn.output.path", "xpn:///xpn/wc-quixote");

		// finalCounts.saveAsHadoopFile (
		// 	"xpn:///xpn/wc-quixote",
		// 	Text.class,
		// 	IntWritable.class,
		// 	ExpandOutputFormat.class
		// );
        
		sc.stop();
	}
}

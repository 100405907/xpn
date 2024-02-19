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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FSDataInputStream;
import org.expand.Expand;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import java.net.URI;

public class testSparkExpand{
	private static final Pattern SPACE = Pattern.compile(" ");
        public static void main(String[] args) {

		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("wc")
                        .set("spark.hadoop.fs.defaultFS", "xpn:///")
                        .set("spark.hadoop.fs.xpn.impl", "org.expand.Expand"));

		SparkSession spark = SparkSession.builder().appName("wc")
			.config("spark.hadoop.fs.defaultFS", "xpn:///")
                        .config("spark.hadoop.fs.xpn.impl", "org.expand.Expand")
			.getOrCreate();
		
		//Expand xpn = null;
		//Configuration conf = null;
		//byte b[] = new byte[524288];
        	String filePath = "xpn:///xpn/newtestwrfile.txt";
		//FSDataInputStream in = null;
		/*try{	
		xpn = new Expand();
                conf = new Configuration();
                URI uri = URI.create("xpn:///");
                conf.set("fs.defaultFS", "xpn:///");
                conf.set("fs.xpn.impl", "Expand");
                xpn.initialize(uri, conf);

		in = xpn.open(new Path(filePath), 65536);
		in.read(b, 0, b.length);

		}catch (Exception e){
			System.out.println(e);
		}
		
		String str[] = new String(b).split(" ");
		List<String> blist = Arrays.asList(str);
		JavaRDD<String> lines = sc.parallelize(blist);*/
		//JavaRDD<Row> lines = spark.read().format("binaryFile").load(filePath).javaRDD();
		//System.out.println(lines.collect());
		/*String arr [] = {"hola", "hola", "hola"};
		List<String> l = Arrays.asList(arr);
		JavaRDD<String> rdd = sc.parallelize(l);
*/
		//System.out.println("HADOOP CONFIGURATION SENT: " + sc.hadoopConfiguration());
		JavaPairRDD<LongWritable, Text> rdd = sc.newAPIHadoopFile(
            		filePath,
            		ExpandInputFormat.class,
            		LongWritable.class,
            		Text.class,
            		sc.hadoopConfiguration()
        	);

		JavaRDD<String> lines = rdd.map(tuple -> tuple._2().toString());

//		JavaRDD<String> lines = sc.textFile(filePath);
		System.out.println(lines.collect());

    		//JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());

    		//JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

    		//JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
        	sc.stop();
	}
}

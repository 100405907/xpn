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

public class loadQuixote {
	public static void main(String[] args) {

		Expand xpn = new Expand();
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "xpn:///");
		conf.set("fs.xpn.impl", "Expand");
		String filePath1 = "xpn:///xpn/wikipedia";
		String input1 = "file:///beegfs/home/javier.garciablas/gsotodos/data/wikipedia";
		// String filePath2 = "xpn:///xpn/wikipedia/wikipedia2";
		// String input2 = "file:///beegfs/home/javier.garciablas/gsotodos/data/wikipedia2";
		// String filePath3 = "xpn:///xpn/wikipedia/wikipedia3";
		// String input3 = "file:///beegfs/home/javier.garciablas/gsotodos/data/wikipedia3";
		// String filePath4 = "xpn:///xpn/wikipedia/wikipedia4";
		// String input4 = "file:///beegfs/home/javier.garciablas/gsotodos/data/wikipedia4";

		try{
			xpn.initialize(URI.create("xpn:///"), conf);
			// xpn.mkdirs(new Path("xpn:///wikipedia/"), FsPermission.getFileDefault());
			xpn.loadFileToExpand(conf, new Path(input1), new Path(filePath1));
			// xpn.loadFileToExpand(xpnconf, new Path(input2), new Path(filePath2));
			// xpn.loadFileToExpand(xpnconf, new Path(input3), new Path(filePath3));
			// xpn.loadFileToExpand(xpnconf, new Path(input4), new Path(filePath4));
		} catch (Exception e) {
			System.out.println("Excepcion en la carga");
		}

	}
}
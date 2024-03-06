package org.expand.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.expand.ExpandRDDFunctions;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

public class ExpandSparkFunctions {

    public static void writeExpand (JavaPairRDD<String, Integer> rdd, String path, Configuration xpnconf){

        JavaRDD<Tuple2<String, Integer>> finalrdd = rdd.map(pair -> new Tuple2<>(pair._1(), pair._2()));
        
        writeExpand (finalrdd, path, xpnconf);
    }

    public static void writeExpand (JavaRDD<Tuple2<String, Integer>> rdd, String path, Configuration xpnconf){
        
        xpnconf.set("xpn.output.path", path);

        ExpandRDDFunctions<String, Integer> func = new ExpandRDDFunctions<String, Integer>(rdd, ClassTag$.MODULE$.apply(String.class), ClassTag$.MODULE$.apply(Integer.class), null);
        
        func.saveAsExpandFile("xpn:///xpn/wc-quixote", ClassTag$.MODULE$.apply(ExpandOutputFormat.class));
    }

}
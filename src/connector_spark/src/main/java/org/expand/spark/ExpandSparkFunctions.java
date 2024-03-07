package org.expand.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.expand.ExpandRDDFunctions;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

public class ExpandSparkFunctions<K, V> {

    public static <K, V> void writeExpand (RDD rdd, String path, Configuration xpnconf){

        JavaRDD<Tuple2<K, V>> finalrdd = rdd.toJavaRDD();
        
        writeExpand(finalrdd, path, xpnconf);
    }

    public static <K, V> void writeExpand (JavaPairRDD<K, V> rdd, String path, Configuration xpnconf){

        JavaRDD<Tuple2<K, V>> finalrdd = rdd.map(pair -> new Tuple2<>(pair._1(), pair._2()));
        
        writeExpand (finalrdd, path, xpnconf);
    }

    public static <K, V> void writeExpand (JavaRDD<Tuple2<K, V>> rdd, String path, Configuration xpnconf){
        
        xpnconf.set("xpn.output.path", path);

        ClassTag<K> keyClassTag = ClassTag$.MODULE$.apply((Class<K>) Object.class);
        ClassTag<V> valueClassTag = ClassTag$.MODULE$.apply((Class<V>) Object.class);

        ExpandRDDFunctions<K, V> func = new ExpandRDDFunctions<K, V>(rdd, keyClassTag, valueClassTag, null);
        
        func.saveAsExpandFile("xpn:///xpn/wc-quixote", ClassTag$.MODULE$.apply(ExpandOutputFormat.class));
    }

}
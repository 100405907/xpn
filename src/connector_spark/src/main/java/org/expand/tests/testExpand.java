package org.expand.tests;

import org.expand.hadoop.Expand;
import org.apache.hadoop.fs.Path;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;

public class testExpand{

        public static void main(String[] args) {

        try{
            Expand xpn = new Expand();
            Configuration conf = new Configuration();
            URI uri = URI.create("xpn:///");
            conf.set("fs.defaultFS", "xpn:///");
            conf.set("fs.xpn.impl", "Expand");
            xpn.initialize(uri, conf);
            System.out.println("ANTES DE COPY");
            xpn.loadFileToExpand(conf, new Path("file:///home/lab/data/2000-0.txt"), new Path("xpn:///xpn/wc/quixotestr"));
            System.out.println("DESPUES DE COPY");
        }catch (Exception e){
            System.out.println(e);
        }
	}
}

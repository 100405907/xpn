package org.expand.tests;

import org.expand.hadoop.Expand;
import org.apache.hadoop.fs.Path;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;

public class rmQuixote{

        public static void main(String[] args) {

        try{
            Expand xpn = new Expand();
            Configuration conf = new Configuration();
            URI uri = URI.create("xpn:///");
            conf.set("fs.defaultFS", "xpn:///");
            conf.set("fs.xpn.impl", "Expand");
            xpn.initialize(uri, conf);
            System.out.println("ANTES DE COPY");
            xpn.delete(new Path("xpn:///xpn/quixote"), false);
            xpn.delete(new Path("xpn:///xpn/wc-quixote"), false);
            System.out.println("DESPUES DE COPY");
        }catch (Exception e){
            System.out.println(e);
        }
	}
}

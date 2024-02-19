package org.expand;

import org.expand.Expand;
import org.apache.hadoop.fs.Path;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;

public class testExpand{

        public static void main(String[] args) {

		byte bw [] = new byte [65536];
		byte br [] = new byte [65536];
		for (int i = 0; i < bw.length; i++){
			if (i % 5 == 0) bw[i] = 'h';
			else if (i % 5 == 1) bw[i] = 'o';
			else if (i % 5 == 2) bw[i] = 'l';
			else if (i % 5 == 3) bw[i] = 'a';
			else if (i % 5 == 4) bw[i] = ' ';
		}

		try{
		Expand xpn = new Expand();
		Configuration conf = new Configuration();
		URI uri = URI.create("xpn:///");
        	conf.set("fs.defaultFS", "xpn:///");
        	conf.set("fs.xpn.impl", "Expand");
		xpn.initialize(uri, conf);

		xpn.delete(new Path("/xpn/testresults"), true);
		xpn.delete(new Path("/xpn/newtestwrfile.txt"), false);
		FSDataOutputStream out = xpn.create(new Path ("/xpn/newtestwrfile.txt"), FsPermission.getFileDefault(),
            	true, 4096, (short) 0, (long) 4096, null);
		out.write(bw);
		out.close();
		xpn.close();
		}catch (Exception e){
			System.out.println(e);
		}
	}
}

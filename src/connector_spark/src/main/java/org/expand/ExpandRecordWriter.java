package org.expand;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import scala.Tuple2;
import java.io.IOException;
import java.util.List;
import java.io.Serializable;

public class ExpandRecordWriter implements RecordWriter<Text, IntWritable>, Serializable {
    private Path outputPath;
    private FSDataOutputStream out;
    // private Expand xpn;

    public ExpandRecordWriter(Configuration conf, Path outputPath) throws IOException {
        this.outputPath = outputPath;
        // this.xpn = new Expand();
        // xpn.initialize(URI.create("xpn:///"), conf);
        System.out.println("--------------------LLEGO A EXPAND RECORD WRITER--------------------");
        this.out = outputPath.getFileSystem(conf).append(outputPath);
    }

    @Override
    public void write(Text key, IntWritable value) throws IOException {
        String towr = "{key: " + key.toString() + ", value: " + value.toString() + "}\n";
        out.write(towr.getBytes());
    }

    @Override
    public void close(Reporter reporter) throws IOException {
        out.close();
    }
}
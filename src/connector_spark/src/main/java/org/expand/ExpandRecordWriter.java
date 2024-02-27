package org.expand;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import scala.Tuple2;
import java.io.IOException;
import java.util.List;

public class ExpandRecordWriter extends RecordWriter<Text, IntWritable> {
    private Path outputPath;
    private FSDataOutputStream out;
    // private Expand xpn;

    public ExpandRecordWriter(Configuration conf, Path outputPath) throws IOException {
        this.outputPath = outputPath;
        // this.xpn = new Expand();
        // xpn.initialize(URI.create("xpn:///"), conf);
        this.out = outputPath.getFileSystem(conf).append(outputPath);
    }

    @Override
    public void write(Text key, IntWritable value) throws IOException, InterruptedException {
        String towr = "{key: " + key.toString() + ", value: " + value.toString() + "}\n";
        out.write(towr.getBytes());
    }

    public void write(Tuple2<Text, IntWritable> [] tuples) throws IOException, InterruptedException{
        
        for (Tuple2<Text, IntWritable> tuple : tuples){
            if (tuple._2().get() > 1){
                String towr = "{key: " + tuple._1().toString() + ", value: " + tuple._2().toString() + "} ";
                out.write(towr.getBytes());
            }
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        out.close();
    }
}
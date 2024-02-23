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
    private Expand xpn;
    private Path outputPath;
    FSDataOutputStream out;

    public ExpandRecordWriter(Configuration conf, Path outputPath) throws IOException {
        this.xpn = new Expand();
        xpn.initialize(URI.create("xpn:///"), conf);
        this.outputPath = outputPath;
        out = xpn.append(outputPath);
    }

    @Override
    public void write(Text key, IntWritable value) throws IOException, InterruptedException {
        String towr = "{key: " + key.toString() + ", value: " + value.toString() + "}\n";
        out.write(towr.getBytes());
    }

    public void write(List<Tuple2<Text, IntWritable>> tuples) throws IOException, InterruptedException{
        
        for (Tuple2<Text, IntWritable> tuple : tuples){
            if (tuple._2().get() > 1){
                String towr = "{key: " + tuple._1().toString() + ", value: " + tuple._2().toString() + "}\n";
                out.write(towr.getBytes());
            }
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        out.close();
    }
}
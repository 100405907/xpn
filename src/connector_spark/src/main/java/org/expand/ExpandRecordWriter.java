package org.expand;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import java.io.IOException;

public class ExpandRecordWriter extends RecordWriter<Text, IntWritable> {
    private Expand xpn;
    private Path outputPath;

    public ExpandRecordWriter(Configuration conf, Path outputPath) throws IOException {
        this.xpn = new Expand();
        xpn.initialize(URI.create("xpn:///"), conf);
        this.outputPath = outputPath;
    }

    @Override
    public void write(Text key, IntWritable value) throws IOException, InterruptedException {
        String towr = "{key: " + key.toString() + ", value: " + value.toString() + "}";
        FSDataOutputStream out = xpn.append(outputPath);
        out.write(towr.getBytes());
        out.close();
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        //xpn.close();
    }
}
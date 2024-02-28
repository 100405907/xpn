package org.expand;

import org.expand.Expand;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import java.net.URI;
import java.io.IOException;


public class ExpandOutputFormat extends FileOutputFormat<Text, IntWritable> {

    private static final String OUTPUT_PATH_KEY = "xpn.output.path";
    
    @Override
    public RecordWriter<Text, IntWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        Path out = new Path(job.getConfiguration().get(OUTPUT_PATH_KEY));
        System.out.println("--------------------LLEGO A EXPAND OUTPUT FORMAT--------------------");
        return new ExpandRecordWriter(job.getConfiguration(), out);
    }

    @Override
    public void checkOutputSpecs(JobContext job) throws IOException {
        
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {
        Path outputPath = new Path(context.getConfiguration().get(OUTPUT_PATH_KEY));
        return new ExpandOutputCommitter(outputPath, context);
    }
}
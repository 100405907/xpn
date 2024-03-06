package org.expand.spark;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.InvalidJobConfException;
import java.io.IOException;

import org.expand.spark.ExpandOutputCommitter;

public class ExpandOutputFormat extends FileOutputFormat<String, Integer> {

    private static final String OUTPUT_PATH_KEY = "xpn.output.path";
    
    @Override
    public RecordWriter<String, Integer> getRecordWriter(FileSystem ignored, JobConf job, String name, Progressable progress) throws IOException {
        Path out = new Path(job.get(OUTPUT_PATH_KEY));
        System.out.println("--------------------LLEGO A EXPAND OUTPUT FORMAT--------------------");
        return new ExpandRecordWriter(job, out);
    }

    @Override
    public void checkOutputSpecs(FileSystem ignored, JobConf job) throws FileAlreadyExistsException, InvalidJobConfException, IOException {
        job.set("mapreduce.outputcommitter.factory.scheme.xpn", "org.expand.spark.ExpandOutputCommitter");
        job.setOutputCommitter(ExpandOutputCommitter.class);
    }

}
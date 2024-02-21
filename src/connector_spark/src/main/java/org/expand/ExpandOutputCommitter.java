package org.expand;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.expand.Expand;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import java.io.IOException;

public class ExpandOutputCommitter extends OutputCommitter {

    private Path outputPath;

    public ExpandOutputCommitter(Path outputPath, TaskAttemptContext context) throws IOException {
        this.outputPath = outputPath;
    }

    @Override
    public void commitJob(JobContext context) throws IOException {
        Configuration conf = context.getConfiguration();
        Expand xpn = new Expand();
        xpn.initialize(URI.create("xpn:///"), conf);

        Path successFile = new Path(outputPath, "_SUCCESS");
        xpn.create(successFile).close();
    }

    @Override
    public void setupJob(JobContext context) throws IOException {
        
    }

    @Override
    public void setupTask(TaskAttemptContext context) throws IOException {
        
    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext context) throws IOException {
        return false;
    }

    @Override
    public void commitTask(TaskAttemptContext context) throws IOException {
        
    }

    @Override
    public void abortTask(TaskAttemptContext context) throws IOException {
        
    }
}
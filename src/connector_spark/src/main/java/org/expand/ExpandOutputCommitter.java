package org.expand;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.expand.Expand;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.TaskAttemptContext;
import java.io.IOException;

public class ExpandOutputCommitter extends OutputCommitter {

    private Path outputPath;

    public ExpandOutputCommitter(Path outputPath, TaskAttemptContext context) throws IOException {
        this.outputPath = outputPath;
    }

    @Override
    public void commitJob(JobContext context) throws IOException {
        
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
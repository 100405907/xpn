package org.expand.spark;

import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.TaskAttemptContext;
import java.io.IOException;

public class ExpandOutputCommitter extends OutputCommitter {

    public ExpandOutputCommitter() throws IOException {
        
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
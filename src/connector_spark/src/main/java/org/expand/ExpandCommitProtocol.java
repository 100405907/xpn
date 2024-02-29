package org.expand;

import org.apache.spark.internal.io.HadoopMapReduceCommitProtocol;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import scala.collection.Seq;
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage;

public class ExpandCommitProtocol extends HadoopMapReduceCommitProtocol {

    public ExpandCommitProtocol(String jobId, String outputPath) {
        super(jobId, outputPath, false);
    }

    @Override
    public void commitJob(JobContext context, Seq<TaskCommitMessage> taskCommits) {
        
    }

    @Override
    public void setupJob(JobContext context) {
        
    }

    @Override
    public void setupTask(TaskAttemptContext context) {
        
    }

    @Override
    public void abortTask(TaskAttemptContext context) {
        
    }

    @Override
    public void abortJob(JobContext job) {
        
    }

}
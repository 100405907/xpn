package org.expand.spark;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.List;

public class ExpandInputFormat extends FileInputFormat<LongWritable, Text> {

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
	    return new ExpandRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return super.isSplitable(context,file);
    }
}


package org.expand;

import java.net.URI;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.expand.Expand;

import java.io.IOException;

public class ExpandRecordReader extends RecordReader<LongWritable, Text> {

    private LongWritable key = new LongWritable();
    private Text value = new Text();
    private long start;
    private long end;
    private long pos;
    Expand xpn;
    private FSDataInputStream fsin;
    private static final int bufsize = 65536;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) split;
        start = fileSplit.getStart();
        end = start + fileSplit.getLength();
        pos = start;
        Path file = fileSplit.getPath();
	System.out.println("RUTA EN RECORDREADER: " + file.toString());
        xpn = new Expand();
        System.out.println("SALGO DE EXPAND()");
	fsin = xpn.open(file);
        fsin.seek(start);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (pos < end) {
            key.set(pos);
	    byte[] buffer = new byte[bufsize];
            int bytesRead = fsin.read(buffer);
            if (bytesRead > 0) {
                value.set(new String(buffer, 0, bytesRead));
                pos += bytesRead;
                return true;
            }
        }
        return false;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
    }

    @Override
    public void close() throws IOException {
        if (fsin != null) {
            fsin.close();
	    //xpn.close();
        }
    }
}


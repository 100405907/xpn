package org.expand;

import java.io.Closeable;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.expand.ExpandInputStream;

public class ExpandFSInputStream extends ExpandInputStream
        implements Closeable, Seekable, PositionedReadable {
    private FileSystem.Statistics statistics;
    public ExpandFSInputStream(String path, int bufferSize,
            FileSystem.Statistics statistics) {
        super(path, bufferSize);
//        this.statistics = statistics;
//        statistics.incrementReadOps(1);
    }

    @Override
    public long getPos(){
//        statistics.incrementReadOps(1);
        return super.tell();
    }

    @Override
    public synchronized int read() {
//        statistics.incrementReadOps(1);
        return super.read();
    }

    @Override
    public synchronized int read(byte[] b) {
//        statistics.incrementReadOps(1);
        return super.read(b);
    }

    @Override
    public synchronized int read(byte[] b, int off, int len) {
        int ret = super.read(b, off, len);
//        statistics.incrementReadOps(1);
        return ret;
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length) {
        long oldPos = getPos();
        seek(position);
        int ret = read(buffer, offset, length);
        seek(oldPos);
        return ret;
    }

    @Override
    public void readFully(long position, byte[] buffer) {
        long oldPos = getPos();
        seek(position);
        int ret = read(buffer);
        seek(oldPos);
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) {
        long oldPos = getPos();
        seek(position);
        int ret = read(buffer, offset, length);
        seek(oldPos);
    }

    @Override
    public synchronized void seek(long pos) {
//        statistics.incrementReadOps(1);
        super.seek(pos);
    }

    @Override
    public synchronized boolean seekToNewSource(long targetPos) {
        return false;
    }
}


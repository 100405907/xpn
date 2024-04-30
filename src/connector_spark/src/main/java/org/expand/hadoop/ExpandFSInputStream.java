package org.expand.hadoop;

import java.io.Closeable;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

public class ExpandFSInputStream extends ExpandInputStream
        implements Closeable, Seekable, PositionedReadable {
    private FileSystem.Statistics statistics;
    public ExpandFSInputStream(String path, int bufferSize,
            FileSystem.Statistics statistics) {
        super(path, bufferSize);
    }

    @Override
    public long getPos(){
        return super.tell();
    }

    @Override
    public synchronized int read() {
        System.out.println("------------------- ENTRO A READ() --------------------");
        return super.read();
    }

    @Override
    public synchronized int read(byte[] b) {
        System.out.println("------------------- ENTRO A READ(b) --------------------");
        return super.read(b);
    }

    @Override
    public synchronized int read(byte[] b, int off, int len) {
        System.out.println("------------------- ENTRO A READ(b, off, len" + len + ") --------------------");
        int ret = super.read(b, off, len);
        System.out.println("------------------- BYTES READ: " + ret + ") --------------------");
        return ret;
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length) {
        System.out.println("------------------- ENTRO A READ(pos, b, off, len: " + length + ") --------------------");
        long oldPos = getPos();
        seek(position);
        int ret = read(buffer, offset, length);
        seek(oldPos);
        return ret;
    }

    @Override
    public void readFully(long position, byte[] buffer) {
        System.out.println("------------------- ENTRO A READF(pos, buf) --------------------");
        long oldPos = getPos();
        seek(position);
        int ret = read(buffer);
        seek(oldPos);
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) {
        System.out.println("------------------- ENTRO A READF(pos, buf, off, len) --------------------");
        long oldPos = getPos();
        seek(position);
        int ret = read(buffer, offset, length);
        seek(oldPos);
    }

    @Override
    public synchronized void seek(long pos) {
        super.seek(pos);
    }

    @Override
    public synchronized boolean seekToNewSource(long targetPos) {
        return false;
    }
}


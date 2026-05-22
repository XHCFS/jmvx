package edu.uic.cs.jmvx.runtime.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class ByteBufferInputStream extends InputStream {

    private MappedByteBuffer mmaped;

    public ByteBufferInputStream(Path toFile) throws IOException {
        FileChannel c = FileChannel.open(toFile, StandardOpenOption.READ);
        this.mmaped = c.map(FileChannel.MapMode.READ_ONLY, 0, c.size());
    }

    public ByteBufferInputStream(MappedByteBuffer mmaped) {
        this.mmaped = mmaped;
    }

    @Override
    public int read() throws IOException {
        return mmaped.get();
    }

    @Override
    public long skip(long n) throws IOException {
        mmaped.position((int)n + mmaped.position());
        return mmaped.position();
    }

    @Override
    public void close() throws IOException {}

    @Override
    public synchronized void mark(int readlimit) {
        mmaped.mark();
    }

    @Override
    public synchronized void reset() throws IOException {
        mmaped.reset();
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public int read(byte[] b, int off, int len) {
        mmaped.get(b, off, len);
        return len;
    }

    @Override
    public int read(byte[] b) {
        mmaped.get(b);
        return b.length;
    }

    @Override
    public int available() {
        return mmaped.remaining();
    }
}

package com.github.nowackia.kafka.connect.file.stream;

import java.io.FilterInputStream;
import java.io.InputStream;
import java.io.IOException;

public class PositionInputStream extends FilterInputStream {

    protected long position = 0;
    private long markedPosition = 0;

    public PositionInputStream(InputStream inputStream) {
        super(inputStream);
    }

    public long getPosition() {
        return position;
    }

    @Override
    public int available() throws IOException {
        return in.available();
    }

    @Override
    public int read() throws IOException {
        int b = in.read();
        if (b != -1)
            position++;
        return b;
    }

    @Override
    public void close() throws IOException {
        in.close();
    }

    @Override
    public void reset() throws IOException {
        in.reset();
        position = markedPosition;
    }

    @Override
    public boolean markSupported() {
        return in.markSupported();
    }

    @Override
    public void mark(int readlimit) {
        in.mark(readlimit);
        markedPosition = position;
    }

    @Override
    public long skip(long n) throws IOException {
        final long c = in.skip(n);
        if (c > 0)
            position += c;
        return c;
    }

    @Override
    public int read(byte b[], int off, int len) throws IOException {
        final int c = in.read(b, off, len);
        if (c > 0)
            position += c;
        return c;
    }
}

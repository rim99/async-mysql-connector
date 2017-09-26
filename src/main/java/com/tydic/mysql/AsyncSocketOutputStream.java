package com.tydic.mysql;

import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PipedOutputStream;

public class AsyncSocketOutputStream
        extends OutputStream {
    private PipedOutputStream pipedOutputStream;
    private final AsyncSocketChannel channel;

    public AsyncSocketOutputStream(AsyncSocketChannel channel) {
        this.channel = channel;
        this.pipedOutputStream = channel.getPipedOutputStream();
    }

    public void write(int b)
            throws IOException {
        write(new byte[]{(byte) b}, 0, 1);
    }

    public void write(byte[] b)
            throws IOException {
        write(b, 0, b.length);
    }

    public void write(byte[] b, int off, int len)
            throws IOException {
        ByteBuf buff = channel.alloc().buffer(len - off);
        buff.writeBytes(b, off, len);
        this.channel.write(buff);
        byte[] mockPacket = this.channel.getMockPacket();
        if (mockPacket != null) {
            this.pipedOutputStream.write(mockPacket);
            this.channel.setMockPacket(null);
            this.pipedOutputStream.flush();
        }
    }

    public void flush()
            throws IOException {
        this.channel.flush();
    }
}

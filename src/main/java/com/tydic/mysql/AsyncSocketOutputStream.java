package com.tydic.mysql;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.io.OutputStream;

public class AsyncSocketOutputStream
        extends OutputStream {
    private final AsyncSocketChannel channel;
    private ByteBuf writeByteBuf;

    public AsyncSocketOutputStream(AsyncSocketChannel channel) {
        this.channel = channel;
    }

    public void write(int b)
            throws IOException {
        write(new byte[]{(byte) b}, 0, 1);
    }

    private void checkChannel() throws IOException {
        if (!channel.isActive()) {
            throw new IOException("connection is closed! " + channel);
        }
    }

    public void write(byte[] b)
            throws IOException {
        write(b, 0, b.length);
    }

    public void write(byte[] b, int off, int len)
            throws IOException {
        checkChannel();
        channel.setInErrorStream(false);
        if (writeByteBuf == null) {
            writeByteBuf = this.channel.alloc().directBuffer();
        }
        writeByteBuf.writeBytes(b, off, len);
    }

    private void checkMock() throws IOException {
        byte[] mockPacket = this.channel.getMockPacket();
        if (mockPacket != null) {
            this.channel.setMockPacket(null);
            this.channel.getInputQueue().offer(Unpooled.wrappedBuffer(mockPacket));
        }
    }

    public void flush() throws IOException {
        checkChannel();
        if (writeByteBuf == null) {
            return;
        }
        channel.selfWrite(writeByteBuf);
        checkMock();
    }

    @Override
    public void close() throws IOException {
        super.close();
        if (writeByteBuf != null) {
            writeByteBuf.release();
            writeByteBuf = null;
        }
    }
}

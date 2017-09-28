package com.tydic.mysql;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PipedOutputStream;
import java.nio.channels.SocketChannel;

public class AsyncSocketOutputStream
        extends OutputStream {
    private PipedOutputStream pipedOutputStream;
    private final AsyncSocketChannel channel;
    private ByteBuf writeByteBuf;

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
        channel.setInErrorStream(false);
//direct write
        if (writeByteBuf == null) {
            writeByteBuf = this.channel.alloc().directBuffer();
        }
        writeByteBuf.writeBytes(b, off, len);

//netty write
//        ByteBuf buff = channel.alloc().buffer(len - off);
//        buff.writeBytes(b, off, len);
//        this.channel.write(buff);
        checkMock();
    }

    private void checkMock() throws IOException {
        byte[] mockPacket = this.channel.getMockPacket();
        if (mockPacket != null) {
            this.pipedOutputStream.write(mockPacket);
            this.channel.setMockPacket(null);
            this.pipedOutputStream.flush();
        }
    }

    public void flush()
            throws IOException {
        if(writeByteBuf == null){
            return;
        }
        channel.selfWrite(writeByteBuf);

//        this.channel.flush();
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

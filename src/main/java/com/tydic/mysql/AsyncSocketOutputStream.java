package com.tydic.mysql;

import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PipedOutputStream;

/**
 * Created by shihailong on 2017/9/21.
 */
public class AsyncSocketOutputStream extends OutputStream {

    private PipedOutputStream pipedOutputStream;

    public AsyncSocketOutputStream(AsyncSocketChannel channel) {
        this.channel = channel;
        pipedOutputStream = channel.getPipedOutputStream();
    }

    private final AsyncSocketChannel channel;

    @Override
    public void write(int b) throws IOException {
        write(new byte[]{(byte)b}, 0, 1);
    }

    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        channel.writeAndFlush(Unpooled.wrappedBuffer(b, off, len));
        byte[] mockPacket = channel.getMockPacket();
        if(mockPacket != null){
            pipedOutputStream.write(mockPacket);
            channel.setMockPacket(null);
            pipedOutputStream.flush();
        }
    }
}

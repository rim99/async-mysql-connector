package com.tydic.mysql;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by shihailong on 2017/9/30.
 */
public class AsyncSocketInputStream extends InputStream {
    private ByteBuf byteBuf;
    private final AsyncSocketChannel channel;

    public AsyncSocketInputStream(AsyncSocketChannel channel) {
        this.channel = channel;
    }

    @Override
    public int read() throws IOException {
        int available = checkReadableByteBuf();
        if (available == 0) {
            return -1;
        }
        return byteBuf.readByte() & 0xFF;
    }

    private int checkReadableByteBuf() throws IOException {
        if(byteBuf == null) {
            byteBuf = take();
            return byteBuf.readableBytes();
        }
        if(!byteBuf.isReadable()){
            byteBuf.release();
            byteBuf = take();
        }
        return byteBuf.readableBytes();
    }

    private ByteBuf take() throws IOException {
        BlockingQueue<ByteBuf> inputQueue = channel.getInputQueue();
        ByteBuf polled;
        while(channel.isActive()){
            try {
                polled = inputQueue.poll(10, TimeUnit.MILLISECONDS);
                if(polled != null){
                    return polled;
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        throw new IOException("connection is closed! " + channel);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int available = checkReadableByteBuf();
        if (available == 0) {
            return -1;
        }

        len = Math.min(available, len);
        byteBuf.readBytes(b, off, len);
        return len;
    }

    @Override
    public int available() throws IOException {
        BlockingQueue<ByteBuf> inputQueue = channel.getInputQueue();
        if(byteBuf != null && (!byteBuf.isReadable())){
            byteBuf.release();
            byteBuf = null;
        }
        if(byteBuf == null && inputQueue.isEmpty()){
            return 0;
        }else{
            return checkReadableByteBuf();
        }
    }

    @Override
    public void close() throws IOException {
        if(byteBuf != null){
            byteBuf.release();
        }
        channel.close();
    }
}

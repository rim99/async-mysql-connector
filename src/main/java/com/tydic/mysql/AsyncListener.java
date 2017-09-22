package com.tydic.mysql;

import com.tydic.mysql.AsyncSocketChannel;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;

import java.io.IOException;

/**
 * Created by shihailong on 2017/9/21.
 */
public abstract class AsyncListener<T> extends ChannelInboundHandlerAdapter {
    protected boolean isEOFDeprecated;
    private boolean inResultSetStream = false;
    private int columnCount;
    protected AsyncSocketChannel channel;
    protected DefaultPromise<T> promise;
    protected boolean init = false;

    public AsyncListener(AsyncSocketChannel asyncSocketChannel) {
        super();
        init(asyncSocketChannel);
    }

    protected AsyncListener() {

    }

    public void init(AsyncSocketChannel asyncSocketChannel) {
        if(init){
            return;
        }
        this.channel = asyncSocketChannel;
        this.isEOFDeprecated = channel.getIO().isEOFDeprecated();
        EventExecutor eventExecutor = asyncSocketChannel.pipeline().lastContext().executor();
        this.promise = new DefaultPromise<T>(eventExecutor);
        init = true;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof ByteBuf) {
            ByteBuf byteBuf = (ByteBuf) msg;
            try {
                channelRead(ctx, byteBuf);
            } finally {
                byteBuf.release();
            }
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    public void channelRead(ChannelHandlerContext ctx, ByteBuf byteBuf) throws Exception {
        if (inResultSetStream) {
            try {
                if (columnCount > 0) {
                    //columns
                    --columnCount;
                    channelReadResultSetPacket(ctx, byteBuf);
                } else {
                    if (columnCount == 0 && (!isEOFDeprecated)) {
                        --columnCount;
                        if (isEOF(byteBuf)) {
                            channelReadEOFPacket(ctx, byteBuf);
                        } else {
                            throw new RuntimeException("为什么不是EOF包?");
                        }
                    } else {
                        if (isEOF(byteBuf)) {
                            channelReadEOFPacket(ctx, byteBuf);
                        } else {
                            channelReadResultSetPacket(ctx, byteBuf);
                        }
                    }
                }

            } finally {
                --columnCount;
            }

        }
        int type = byteBuf.getByte(4) & 0xFF;
        switch (type) {
            case 0:
                channelReadOKPacket(ctx, byteBuf);
                break;
            case 0xFE:
                channelReadEOFPacket(ctx, byteBuf);
                break;
            case 0xFF:
                channelReadErrorPacket(ctx, byteBuf);
                break;
            case 0xFC:
                type = (byteBuf.getByte(5) & 0xFF) | ((byteBuf.getByte(6) & 0xFF) << 8);
            default:
                inResultSetStream = true;
                columnCount = type;
                channelReadResultSetPacket(ctx, byteBuf);
                break;
        }
    }

    private static boolean isEOF(ByteBuf byteBuf) {
        return ((byteBuf.getByte(4) & 0xFF) == 0xFE) && byteBuf.readableBytes() <= 9;
    }

    protected void channelReadResultSetPacket(ChannelHandlerContext ctx, ByteBuf byteBuf) {
        promise.setFailure(new IOException("非预期的报文"));
    }

    protected void channelReadErrorPacket(ChannelHandlerContext ctx, ByteBuf error) {
        promise.setFailure(new IOException("非预期的报文"));
    }

    protected void channelReadEOFPacket(ChannelHandlerContext ctx, ByteBuf eof) {
        promise.setFailure(new IOException("非预期的报文"));
    }

    protected void channelReadOKPacket(ChannelHandlerContext ctx, ByteBuf ok) {
        promise.setFailure(new IOException("非预期的报文"));
    }

    public Future<T> getFuture() {
        return promise;
    }
}

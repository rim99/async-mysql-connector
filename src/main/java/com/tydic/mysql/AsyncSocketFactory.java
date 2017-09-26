package com.tydic.mysql;

import com.mysql.jdbc.SocketFactory;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.io.IOException;
import java.io.PipedOutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.util.Properties;

/**
 * Created by shihailong on 2017/9/21.
 */
public class AsyncSocketFactory implements SocketFactory {
    private static Bootstrap nettyBootstrap = new Bootstrap();

    public static final NioEventLoopGroup EVENT_EXECUTORS = new NioEventLoopGroup();

    public static final String DEFAULT_INBOUND_HANDLER = "DEFAULT_INBOUND_HANDLER";

    public static final String DEFAULT_OUTBOUND_HANDLER = "DEFAULT_OUTBOUND_HANDLER";

    static {
        nettyBootstrap.group(EVENT_EXECUTORS).channel(AsyncSocketChannel.class);
        nettyBootstrap.option(ChannelOption.SO_KEEPALIVE, true);
        nettyBootstrap.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        ChannelInitializer<AsyncSocketChannel> channelInitializer = new ChannelInitializer<AsyncSocketChannel>() {
            @Override
            protected void initChannel(final AsyncSocketChannel ch) throws Exception {
                final PipedOutputStream pipedOutputStream = ch.getPipedOutputStream();
                ch.pipeline().addLast(DEFAULT_INBOUND_HANDLER, new ChannelInboundHandlerAdapter() {

                    @Override
                    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                        ch.getPipedOutputStream().close();
                        ch.getInputStream().close();
                        ch.close();
                    }

                    @Override
                    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                        if (msg instanceof ByteBuf) {
                            ByteBuf byteBuf = (ByteBuf) msg;
                            byteBuf.readBytes(pipedOutputStream, byteBuf.readableBytes());
                            byteBuf.release();
                            pipedOutputStream.flush();
                        } else {
                            super.channelRead(ctx, msg);
                        }
                    }
                });
                ch.pipeline().addLast(DEFAULT_OUTBOUND_HANDLER, new ChannelOutboundHandlerAdapter() {

                    @Override
                    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
                        super.write(ctx, msg, promise);
                    }
                });
            }
        };
        nettyBootstrap.handler(channelInitializer);
    }
    /** The underlying TCP/IP socket to use */
    protected Socket rawSocket = null;

    public Socket afterHandshake() throws IOException {
        return rawSocket;
    }

    public Socket beforeHandshake() throws IOException {
        return rawSocket;
    }

    public Socket connect(String host, int portNumber, Properties props) throws IOException {
        try {
            AsyncSocketChannel channel = (AsyncSocketChannel) nettyBootstrap.connect(host, portNumber).sync().channel();
//            channel.deregister().sync();
            rawSocket = new AsyncSocket(channel);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return rawSocket;
    }
}

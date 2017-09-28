package com.tydic.mysql;

import com.mysql.jdbc.SocketFactory;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.internal.SystemPropertyUtil;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Log4J2LoggerFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.PipedOutputStream;
import java.net.Socket;
import java.util.Properties;

/**
 * Created by shihailong on 2017/9/21.
 */
public class AsyncSocketFactory implements SocketFactory {
    private static final Log logger = LogFactory.getLog(AsyncSocketFactory.class);

    private static Bootstrap nettyBootstrap = new Bootstrap();

    private static int DEFAULT_EVENT_LOOP_THREADS = Math.max(4, SystemPropertyUtil.getInt(
            "io.netty.eventLoopThreads", Runtime.getRuntime().availableProcessors()));

    public static final NioEventLoopGroup EVENT_EXECUTORS = new NioEventLoopGroup(DEFAULT_EVENT_LOOP_THREADS,
            new DefaultThreadFactory("async-mysql"));

    public static final String DEFAULT_INBOUND_HANDLER = "DEFAULT_INBOUND_HANDLER";

    public static final String DEFAULT_OUTBOUND_HANDLER = "DEFAULT_OUTBOUND_HANDLER";

    public static final String DEFAULT_LOG_HANDLER = "DEFAULT_LOG_HANDLER";

    static {
        InternalLoggerFactory.setDefaultFactory(Log4J2LoggerFactory.INSTANCE);
        nettyBootstrap.group(EVENT_EXECUTORS).channel(AsyncSocketChannel.class);
        nettyBootstrap.option(ChannelOption.SO_KEEPALIVE, true);
        nettyBootstrap.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        ChannelInitializer<AsyncSocketChannel> channelInitializer = new ChannelInitializer<AsyncSocketChannel>() {
            @Override
            protected void initChannel(final AsyncSocketChannel ch) throws Exception {
                final PipedOutputStream pipedOutputStream = ch.getPipedOutputStream();
                ch.pipeline().addLast(DEFAULT_LOG_HANDLER, new LoggingHandler(LogLevel.INFO));
                ch.pipeline().addLast(DEFAULT_INBOUND_HANDLER, new ChannelInboundHandlerAdapter() {

                    @Override
                    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                        ch.getPipedOutputStream().close();
                        ch.getInputStream().close();
                        ch.doClose();
                        logger.warn(ctx + " inactive");
                    }

                    @Override
                    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                        if(ch.isInErrorStream()){
                            ReferenceCountUtil.release(msg);
                            return;
                        }
                        if (msg instanceof ByteBuf) {
                            ByteBuf byteBuf = (ByteBuf) msg;
                            byteBuf.readBytes(pipedOutputStream, byteBuf.readableBytes());
                            byteBuf.release();
                            pipedOutputStream.flush();
                        } else {
                            super.channelRead(ctx, msg);
                        }
                    }

                    @Override
                    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                        logger.error(cause);
                        ch.getPipedOutputStream().close();
                        ch.getInputStream().close();
                        ch.doClose();
                    }
                });
                ch.pipeline().addLast(DEFAULT_OUTBOUND_HANDLER, new ChannelOutboundHandlerAdapter() {

                    @Override
                    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                        logger.error(cause);
                        ch.getPipedOutputStream().close();
                        ch.getInputStream().close();
                        ch.doClose();
                    }

                    @Override
                    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
                        super.write(ctx, msg, promise);
                    }
                });
            }
        };
        nettyBootstrap.handler(channelInitializer);
    }

    /**
     * The underlying TCP/IP socket to use
     */
    protected Socket rawSocket = null;

    public Socket afterHandshake() throws IOException {
        return rawSocket;
    }

    public Socket beforeHandshake() throws IOException {
        return rawSocket;
    }

    public synchronized Socket connect(String host, int portNumber, Properties props) throws IOException {
        try {
            AsyncSocketChannel channel = (AsyncSocketChannel) nettyBootstrap.connect(host, portNumber).sync().channel();
            rawSocket = new AsyncSocket(channel);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return rawSocket;
    }
}

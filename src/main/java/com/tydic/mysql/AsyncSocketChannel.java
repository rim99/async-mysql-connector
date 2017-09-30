package com.tydic.mysql;

import com.mysql.jdbc.MySQLConnection;
import com.mysql.jdbc.MysqlIO;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by shihailong on 2017/9/21.
 */
public class AsyncSocketChannel extends NioSocketChannel {
    private MysqlIO io;
    private byte[] mockPacket;
    private Object connectionMutex;
    private volatile boolean inErrorStream;
    private BlockingQueue<ByteBuf> inputQueue = new LinkedBlockingDeque<>();

    private boolean async;
    private AsyncSocketOutputStream asyncSocketOutputStream = new AsyncSocketOutputStream(this);
    ;
    private AsyncSocketInputStream asyncSocketInputStream = new AsyncSocketInputStream(this);

    public MySQLConnection getMySQLConnection() {
        return mySQLConnection;
    }

    public void setMySQLConnection(MySQLConnection mySQLConnection) {
        this.mySQLConnection = mySQLConnection;
    }

    private MySQLConnection mySQLConnection;

    public AsyncSocketChannel() {
        super();
    }

    public AsyncSocketChannel(SelectorProvider provider) {
        super(provider);
    }

    public AsyncSocketChannel(SocketChannel socket) {
        super(socket);
    }

    public AsyncSocketChannel(Channel parent, SocketChannel socket) {
        super(parent, socket);
    }

    public SocketChannel javaChannel() {
        return super.javaChannel();
    }

    @Override
    protected void doClose() throws Exception {
        super.doClose();
        if (mySQLConnection != null) {
            mySQLConnection.close();
        }
        asyncSocketOutputStream.close();
    }

    public OutputStream getOutputStream() {
        return asyncSocketOutputStream;
    }

    public InputStream getInputStream() {
        return asyncSocketInputStream;
    }

    public void setIO(MysqlIO io) {
        this.io = io;
    }

    public MysqlIO getIO() {
        return io;
    }

    public void setMockPacket(byte[] packet) {
        this.mockPacket = packet;
    }

    public byte[] getMockPacket() {
        return mockPacket;
    }

    public Object getConnectionMutex() {
        return connectionMutex;
    }

    public void setConnectionMutex(Object connectionMutex) {
        this.connectionMutex = connectionMutex;
    }

    public boolean isAsync() {
        return async;
    }

    public void setAsync(boolean async) {
        this.async = async;
    }

    void selfWrite(ByteBuf byteBuf) throws IOException {
        ByteBuffer[] nioBuffers = byteBuf.nioBuffers();
        int nioBufferCnt = byteBuf.nioBufferCount();
        long expectedWrittenBytes = byteBuf.readableBytes();
        SocketChannel ch = javaChannel();
        // Always us nioBuffers() to workaround data-corruption.
        // See https://github.com/netty/netty/issues/2761
        while (expectedWrittenBytes > 0) {
            switch (nioBufferCnt) {
                case 0:
                    return;
                case 1:
                    // Only one ByteBuf so use non-gathering write
                    ByteBuffer nioBuffer = nioBuffers[0];
                    for (int i = config().getWriteSpinCount() - 1; i >= 0; i--) {
                        final int localWrittenBytes = ch.write(nioBuffer);
                        if (localWrittenBytes == 0) {
                            throw new RuntimeException("write 0 bytes to " + remoteAddress().toString());
                        }
                        expectedWrittenBytes -= localWrittenBytes;
                        if (expectedWrittenBytes == 0) {
                            break;
                        }
                    }
                    break;
                default:
                    for (int i = config().getWriteSpinCount() - 1; i >= 0; i--) {
                        final long localWrittenBytes = ch.write(nioBuffers, 0, nioBufferCnt);
                        if (localWrittenBytes == 0) {
                            throw new RuntimeException("write 0 bytes to " + remoteAddress().toString());
                        }
                        expectedWrittenBytes -= localWrittenBytes;
                        if (expectedWrittenBytes == 0) {
                            break;
                        }
                    }
                    break;
            }
        }
        byteBuf.clear();
    }

    public boolean isInErrorStream() {
        return inErrorStream;
    }

    public void setInErrorStream(boolean inErrorStream) {
        this.inErrorStream = inErrorStream;
    }

    public BlockingQueue<ByteBuf> getInputQueue() {
        return inputQueue;
    }
}

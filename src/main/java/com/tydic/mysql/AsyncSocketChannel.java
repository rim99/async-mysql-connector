package com.tydic.mysql;

import com.mysql.jdbc.MySQLConnection;
import com.mysql.jdbc.MysqlIO;
import io.netty.channel.Channel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.io.*;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;

/**
 * Created by shihailong on 2017/9/21.
 */
public class AsyncSocketChannel extends NioSocketChannel {
    private MysqlIO io;
    private byte[] mockPacket;
    private Object connectionMutex;

    private boolean async;

    public MySQLConnection getMySQLConnection() {
        return mySQLConnection;
    }

    public void setMySQLConnection(MySQLConnection mySQLConnection) {
        this.mySQLConnection = mySQLConnection;
    }

    private MySQLConnection mySQLConnection;

    public AsyncSocketChannel() {
        try {
            pipedOutputStream.connect(pipedInputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
        mySQLConnection.close();
        super.doClose();
    }

    public OutputStream getOutputStream() {
        return new AsyncSocketOutputStream(this);
    }

    private PipedInputStream pipedInputStream = new PipedInputStream();

    public PipedOutputStream getPipedOutputStream() {
        return pipedOutputStream;
    }

    private PipedOutputStream pipedOutputStream = new PipedOutputStream();

    public InputStream getInputStream() {
        return pipedInputStream;
    }

    public void setIO(MysqlIO io) {
        this.io = io;
    }

    public MysqlIO getIO(){
        return io;
    }

    public void setMockPacket(byte[] packet){
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
}

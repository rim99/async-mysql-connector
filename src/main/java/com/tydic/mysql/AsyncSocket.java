package com.tydic.mysql;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.*;
import java.nio.channels.SocketChannel;

/**
 * Created by shihailong on 2017/9/21.
 */
public class AsyncSocket extends Socket {
    private final AsyncSocketChannel channel;
    private final Socket rawSocket;

    AsyncSocket(AsyncSocketChannel channel) throws SocketException {
        super((SocketImpl) null);
        this.channel = channel;
        this.rawSocket = channel.javaChannel().socket();
    }

    @Override
    public void connect(SocketAddress endpoint) throws IOException {
        rawSocket.connect(endpoint);
    }

    @Override
    public void connect(SocketAddress endpoint, int timeout) throws IOException {
        rawSocket.connect(endpoint, timeout);
    }

    @Override
    public void bind(SocketAddress bindpoint) throws IOException {
        rawSocket.bind(bindpoint);
    }

    @Override
    public InetAddress getInetAddress() {
        return rawSocket.getInetAddress();
    }

    @Override
    public InetAddress getLocalAddress() {
        return rawSocket.getLocalAddress();
    }

    @Override
    public int getPort() {
        return rawSocket.getPort();
    }

    @Override
    public int getLocalPort() {
        return rawSocket.getLocalPort();
    }

    @Override
    public SocketAddress getRemoteSocketAddress() {
        return rawSocket.getRemoteSocketAddress();
    }

    @Override
    public SocketAddress getLocalSocketAddress() {
        return rawSocket.getLocalSocketAddress();
    }

    @Override
    public SocketChannel getChannel() {
        return rawSocket.getChannel();
    }

    @Override
    public InputStream getInputStream() throws IOException {
        return channel.getInputStream();
    }

    @Override
    public OutputStream getOutputStream() throws IOException {
        return channel.getOutputStream();
    }

    @Override
    public void setTcpNoDelay(boolean on) throws SocketException {
        rawSocket.setTcpNoDelay(on);
    }

    @Override
    public boolean getTcpNoDelay() throws SocketException {
        return rawSocket.getTcpNoDelay();
    }

    @Override
    public void setSoLinger(boolean on, int linger) throws SocketException {
        rawSocket.setSoLinger(on, linger);
    }

    @Override
    public int getSoLinger() throws SocketException {
        return rawSocket.getSoLinger();
    }

    @Override
    public void sendUrgentData(int data) throws IOException {
        rawSocket.sendUrgentData(data);
    }

    @Override
    public void setOOBInline(boolean on) throws SocketException {
        rawSocket.setOOBInline(on);
    }

    @Override
    public boolean getOOBInline() throws SocketException {
        return rawSocket.getOOBInline();
    }

    @Override
    public void setSoTimeout(int timeout) throws SocketException {
        rawSocket.setSoTimeout(timeout);
    }

    @Override
    public int getSoTimeout() throws SocketException {
        return rawSocket.getSoTimeout();
    }

    @Override
    public void setSendBufferSize(int size) throws SocketException {
        rawSocket.setSendBufferSize(size);
    }

    @Override
    public int getSendBufferSize() throws SocketException {
        return rawSocket.getSendBufferSize();
    }

    @Override
    public void setReceiveBufferSize(int size) throws SocketException {
        rawSocket.setReceiveBufferSize(size);
    }

    @Override
    public int getReceiveBufferSize() throws SocketException {
        return rawSocket.getReceiveBufferSize();
    }

    @Override
    public void setKeepAlive(boolean on) throws SocketException {
        rawSocket.setKeepAlive(on);
    }

    @Override
    public boolean getKeepAlive() throws SocketException {
        return rawSocket.getKeepAlive();
    }

    @Override
    public void setTrafficClass(int tc) throws SocketException {
        rawSocket.setTrafficClass(tc);
    }

    @Override
    public int getTrafficClass() throws SocketException {
        return rawSocket.getTrafficClass();
    }

    @Override
    public void setReuseAddress(boolean on) throws SocketException {
        rawSocket.setReuseAddress(on);
    }

    @Override
    public boolean getReuseAddress() throws SocketException {
        return rawSocket.getReuseAddress();
    }

    @Override
    public void close() throws IOException {
        rawSocket.close();
    }

    @Override
    public void shutdownInput() throws IOException {
        rawSocket.shutdownInput();
    }

    @Override
    public void shutdownOutput() throws IOException {
        rawSocket.shutdownOutput();
    }

    @Override
    public String toString() {
        return rawSocket.toString();
    }

    @Override
    public boolean isConnected() {
        return rawSocket.isConnected();
    }

    @Override
    public boolean isBound() {
        return rawSocket.isBound();
    }

    @Override
    public boolean isClosed() {
        return rawSocket.isClosed();
    }

    @Override
    public boolean isInputShutdown() {
        return rawSocket.isInputShutdown();
    }

    @Override
    public boolean isOutputShutdown() {
        return rawSocket.isOutputShutdown();
    }

    @Override
    public void setPerformancePreferences(int connectionTime, int latency, int bandwidth) {
        rawSocket.setPerformancePreferences(connectionTime, latency, bandwidth);
    }

    public AsyncSocketChannel getAsyncSocketChannel() {
        return channel;
    }
}

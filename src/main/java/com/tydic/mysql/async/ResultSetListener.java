package com.tydic.mysql.async;

import com.mysql.jdbc.AsyncUtils;
import com.mysql.jdbc.StatementImpl;
import com.tydic.mysql.AsyncListener;
import com.tydic.mysql.AsyncSocketChannel;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

import java.io.IOException;
import java.io.PipedOutputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by shihailong on 2017/9/21.
 */
public class ResultSetListener extends AsyncListener<ResultSet> {
    private PipedOutputStream pipedOutputStream;
    private Statement statement;

    public ResultSetListener(AsyncSocketChannel asyncSocketChannel, Statement statement) {
        super(asyncSocketChannel);
        this.statement = statement;
        this.pipedOutputStream = asyncSocketChannel.getPipedOutputStream();
    }

    public ResultSetListener(Statement statement) {
        super();
        this.statement = statement;
    }

    @Override
    public void init(AsyncSocketChannel asyncSocketChannel) {
        super.init(asyncSocketChannel);
        this.pipedOutputStream = asyncSocketChannel.getPipedOutputStream();
    }

    @Override
    protected void channelReadResultSetPacket(ChannelHandlerContext ctx, ByteBuf byteBuf) {
        try {
            byteBuf.readBytes(pipedOutputStream, byteBuf.readableBytes());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void channelReadErrorPacket(ChannelHandlerContext ctx, ByteBuf error) {
        try {
            AsyncUtils.checkErrorPacket(channel.getIO(), error);
        } catch (SQLException e) {
            promise.setFailure(e);
        }
    }

    @Override
    protected void channelReadEOFPacket(ChannelHandlerContext ctx, ByteBuf eof) {
        try {
            eof.readBytes(pipedOutputStream, eof.readableBytes());
            pipedOutputStream.flush();
            if (isEOFDeprecated) {
                try {
                    promise.setSuccess(AsyncUtils.build((StatementImpl) statement));
                } catch (SQLException e) {
                    promise.setFailure(e);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

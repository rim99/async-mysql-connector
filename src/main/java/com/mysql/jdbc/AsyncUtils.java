package com.mysql.jdbc;

import io.netty.buffer.ByteBuf;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by shihailong on 2017/9/21.
 */
public class AsyncUtils {
    public static ResultSet build(StatementImpl callingStatement) throws SQLException {
        MysqlIO io = callingStatement.connection.getIO();
        Buffer packet = io.readPacket();
        packet.setPosition(1);
        return io.readAllResults(callingStatement, callingStatement.getMaxRows(),
                callingStatement.getResultSetType(),
                callingStatement.resultSetConcurrency,
                false, callingStatement.connection.getCatalog(),
                packet, callingStatement instanceof ServerPreparedStatement,
                -1, null);
    }
    private static final Method checkErrorPacket ;
    static{
        try {
            checkErrorPacket = MysqlIO.class.getDeclaredMethod("checkErrorPacket", Buffer.class);
            checkErrorPacket.setAccessible(true);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }
    public static void checkErrorPacket(MysqlIO io, ByteBuf error) throws SQLException{
        error.skipBytes(4);
        int length = error.readableBytes();
        byte[] buff = new byte[length];
        error.readBytes(buff);
        Buffer packet = new Buffer(buff);
        try {
            checkErrorPacket.invoke(io, packet);
        } catch (IllegalAccessException e) {
            throw new SQLException(e);
        } catch (InvocationTargetException e) {
            throw new SQLException(e);
        }
    }
}

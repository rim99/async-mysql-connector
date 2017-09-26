package com.tydic.mysql;

import com.mysql.jdbc.*;
import com.tydic.mysql.async.MySQLBufferFrameDecoder;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoop;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

/**
 * Created by shihailong on 2017/9/22.
 */
public class AsyncStatementInterceptor implements StatementInterceptorV2 {
    public static final String MY_SQL_BUFFER_FRAME_DECODER_NAME = "MY_SQL_BUFFER_FRAME_DECODER";
    public static final String TMP_LISTENER_NAME = "TMP_LISTENER";
    public static byte[] OK = new byte[]{7, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0};
    static final Map<java.sql.Connection, AsyncStatementInterceptor> CONNECTION_INTERCEPTOR_MAP = new ConcurrentHashMap<>();

    private AsyncSocketChannel channel;
    private boolean init = false;
    private MySQLConnection mySQLConnection;
    private Statement interceptStatement;
    private AsyncListener listener;
    private EventLoop eventLoop;

    public void init(Connection conn, Properties props) throws SQLException {
        this.mySQLConnection = conn.unwrap(MySQLConnection.class);
        CONNECTION_INTERCEPTOR_MAP.put(mySQLConnection, this);
    }

    private void init() throws SQLException {
        if (init) {
            return;
        }
        MysqlIO io = mySQLConnection.getIO();
        AsyncSocket asyncSocket = (AsyncSocket) io.mysqlConnection;
        channel = asyncSocket.getAsyncSocketChannel();
        channel.setIO(io);
        channel.setMySQLConnection(mySQLConnection);
        channel.setConnectionMutex(mySQLConnection.getConnectionMutex());
        init = true;
    }

    private static AsyncStatementInterceptor getAsyncStatementInterceptor(java.sql.Statement statement) throws SQLException {
        java.sql.Connection connection = statement.getConnection();
        connection = connection.unwrap(MySQLConnection.class);
        return CONNECTION_INTERCEPTOR_MAP.get(connection);
    }

    public static <T> Future<T> intercept(EventLoop eventLoop, java.sql.Statement statement, AsyncListener<T> listener) throws SQLException {
        AsyncStatementInterceptor interceptor = getAsyncStatementInterceptor(statement);
        if (!interceptor.init) {
            interceptor.init();
        }
        interceptor.eventLoop = eventLoop;
        return intercept(interceptor, statement, listener);
    }

    public static <T> Future<T> intercept(java.sql.Statement statement, AsyncListener<T> listener) throws SQLException {
        AsyncStatementInterceptor interceptor = getAsyncStatementInterceptor(statement);
        if (!interceptor.init) {
            interceptor.init();
        }
        return intercept(interceptor, statement, listener);
    }

    private static <T> Future<T> intercept(AsyncStatementInterceptor interceptor, java.sql.Statement statement, AsyncListener<T> listener) throws SQLException {
        interceptor.interceptStatement = statement.unwrap(Statement.class);
        EventLoop eventLoop = interceptor.eventLoop;
        if(eventLoop == null){
            eventLoop = interceptor.channel.eventLoop();
        }
        listener.init(interceptor.channel, eventLoop);
        interceptor.listener = listener;
        return listener.getFuture();
    }

    public ResultSetInternalMethods preProcess(String sql, Statement interceptedStatement, Connection connection) throws SQLException {
        if (!init) {
            return null;
        }
        if (this.listener != null && interceptStatement == interceptedStatement) {
            final ChannelPipeline pipeline = channel.pipeline();
            pipeline.addFirst(this.eventLoop, TMP_LISTENER_NAME, listener);
            pipeline.addFirst(this.eventLoop, MY_SQL_BUFFER_FRAME_DECODER_NAME, new MySQLBufferFrameDecoder());
            channel.setMockPacket(OK);
            //noinspection unchecked
            listener.getFuture().addListener(new GenericFutureListener<Future<?>>() {
                ChannelPipeline pipeline = channel.pipeline();

                public void operationComplete(Future<?> future) throws Exception {
                    pipeline.remove(TMP_LISTENER_NAME);
                    pipeline.remove(MY_SQL_BUFFER_FRAME_DECODER_NAME);
                    channel.setAsync(false);
                }
            });
            this.channel.setAsync(true);
            this.interceptStatement = null;
            this.listener = null;
            this.eventLoop = null;
        }else{
            this.channel.setAsync(false);
        }
        return null;
    }

    public boolean executeTopLevelOnly() {
        return true;
    }

    public void destroy() {
        CONNECTION_INTERCEPTOR_MAP.remove(mySQLConnection);
    }

    public ResultSetInternalMethods postProcess(String sql, Statement interceptedStatement, ResultSetInternalMethods originalResultSet, Connection connection, int warningCount, boolean noIndexUsed, boolean noGoodIndexUsed, SQLException statementException) throws SQLException {
        return null;
    }
}

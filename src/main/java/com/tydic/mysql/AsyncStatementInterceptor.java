package com.tydic.mysql;

import com.mysql.jdbc.*;
import com.tydic.mysql.async.MySQLBufferFrameDecoder;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by shihailong on 2017/9/22.
 */
public class AsyncStatementInterceptor implements StatementInterceptorV2 {
    public static final String MY_SQL_BUFFER_FRAME_DECODER_NAME = "MY_SQL_BUFFER_FRAME_DECODER";
    public static final String TMP_LISTENER_NAME = "TMP_LISTENER";
    public static byte[] OK = new byte[]{7, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0};
    private static final Log logger = LogFactory.getLog(AsyncStatementInterceptor.class);

    private AsyncSocketChannel channel;
    private MySQLConnection mySQLConnection;
    private Statement interceptStatement;
    private AsyncListener listener;
    private EventLoop eventLoop;
    private MySQLConnection activeMySQLConnection;

    public void init(Connection conn, Properties props) throws SQLException {
        this.mySQLConnection = conn.unwrap(MySQLConnection.class);
    }

    private void setCurrentActiveMySQLConnection() throws SQLException {
        activeMySQLConnection = mySQLConnection.getActiveMySQLConnection();
    }

    private void setCurrentAsyncSocketChannel() throws SQLException {
        MysqlIO io = activeMySQLConnection.getIO();
        AsyncSocket asyncSocket = (AsyncSocket) io.mysqlConnection;
        channel = asyncSocket.getAsyncSocketChannel();
        channel.setIO(io);
        channel.setMySQLConnection(activeMySQLConnection);
        channel.setConnectionMutex(this);
    }

    private static AsyncStatementInterceptor getAsyncStatementInterceptor(java.sql.Statement statement) throws SQLException {
        java.sql.Connection connection = statement.getConnection();
        MySQLConnection mySQLConnection = connection.unwrap(MySQLConnection.class);
        for (StatementInterceptorV2 statementInterceptorV2 : mySQLConnection.getStatementInterceptorsInstances()) {
            if (statementInterceptorV2 instanceof AsyncStatementInterceptor) {
                return (AsyncStatementInterceptor) statementInterceptorV2;
            }
        }
        throw new RuntimeException("not found AsyncStatementInterceptor!");
    }

    public static <T> Future<T> intercept(EventLoop eventLoop, java.sql.Statement statement, AsyncListener<T> listener) throws SQLException {
        AsyncStatementInterceptor interceptor = getAsyncStatementInterceptor(statement);
        interceptor.eventLoop = eventLoop;
        return intercept(interceptor, statement, listener);
    }

    public static <T> Future<T> intercept(java.sql.Statement statement, AsyncListener<T> listener) throws SQLException {
        AsyncStatementInterceptor interceptor = getAsyncStatementInterceptor(statement);
        return intercept(interceptor, statement, listener);
    }

    private static <T> Future<T> intercept(AsyncStatementInterceptor interceptor, java.sql.Statement statement, AsyncListener<T> listener) throws SQLException {
        interceptor.interceptStatement = statement.unwrap(Statement.class);
        interceptor.setCurrentActiveMySQLConnection();
        interceptor.setCurrentAsyncSocketChannel();
        EventLoop eventLoop = interceptor.eventLoop;
        if (eventLoop == null) {
            eventLoop = interceptor.channel.eventLoop();
        }
        listener.init(interceptor.channel, eventLoop);
        interceptor.listener = listener;
        return listener.getFuture();
    }

    public ResultSetInternalMethods preProcess(String sql, Statement interceptedStatement, Connection connection) throws SQLException {
        if (logger.isInfoEnabled()) {
            logger.info("preProcess " + sql);
        }
        if (this.channel == null || this.listener == null || !isInterceptedStatement(interceptedStatement)) {
            return null;
        }

        final ChannelPipeline pipeline = channel.pipeline();
        pipeline.addAfter(this.eventLoop, AsyncSocketFactory.DEFAULT_LOG_HANDLER, TMP_LISTENER_NAME, listener);
        pipeline.addAfter(this.eventLoop, AsyncSocketFactory.DEFAULT_LOG_HANDLER, MY_SQL_BUFFER_FRAME_DECODER_NAME, new MySQLBufferFrameDecoder());
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
        return null;
    }

    private boolean isInterceptedStatement(Statement interceptedStatement) {
        // proxy 时,怎么判断是否拦截的问题
        return interceptStatement == interceptedStatement || interceptedStatement.toString().equals(interceptStatement.toString());
    }

    public boolean executeTopLevelOnly() {
        return true;
    }

    public void destroy() {

    }

    public ResultSetInternalMethods postProcess(String sql, Statement interceptedStatement, ResultSetInternalMethods originalResultSet, Connection connection, int warningCount, boolean noIndexUsed, boolean noGoodIndexUsed, SQLException statementException) throws SQLException {
        if (logger.isInfoEnabled()) {
            logger.info("postProcess " + sql);
        }
        return null;
    }
}

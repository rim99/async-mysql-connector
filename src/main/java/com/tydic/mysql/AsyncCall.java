package com.tydic.mysql;

import com.tydic.mysql.async.ResultSetListener;
import com.tydic.mysql.async.UpdateCountListener;
import io.netty.util.concurrent.Future;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by shihailong on 2017/9/21.
 */
public class AsyncCall {
    private static String ASYNC_DEFINE = "socketFactory="+AsyncSocketFactory.class.getName()+
            "&statementInterceptors="+AsyncStatementInterceptor.class.getName()+
            "&useSSL=false";

    public static String enable(String url){
        url = url.trim();
        if(url.lastIndexOf('?') == -1){
            return url + "?" + ASYNC_DEFINE;
        }else{
            return url + "&" + ASYNC_DEFINE;
        }
    }

    /**
     * 为Statement对象绑定监听, 使其异步执行.
     *
     * @param statement 原始mysql语句对象
     * @param listener  返回结果监听器
     * @param <T>       返回结果类型, ResultSet或Integer. 或者其他
     * @return Future对象, 可get()获取结果, 也可以注册回调方法.
     * @throws SQLException
     */
    public static <T> Future<T> bind(Statement statement, AsyncListener<T> listener) throws SQLException {
        return AsyncStatementInterceptor.intercept(statement, listener);
    }

    /**
     * 异步更新
     *
     * @param statement 原始mysql语句对象
     * @return Future对象, 可get()获取结果, 也可以注册回调方法.
     * @throws SQLException
     */

    public static Future<Integer> asyncUpdate(PreparedStatement statement) throws SQLException {
        return asyncUpdate(statement, null);
    }

    public static Future<Integer> asyncUpdate(Statement statement, String sql) throws SQLException {
        Future<Integer> future = update(statement);
        executeSilence(statement, sql);
        return future;
    }

    /**
     * 异步查询
     *
     * @param statement 原始mysql语句对象
     * @return Future对象, 可get()获取结果, 也可以注册回调方法.
     * @throws SQLException
     */
    public static Future<ResultSet> asyncQuery(PreparedStatement statement) throws SQLException {
        return asyncQuery(statement, null);
    }

    public static Future<ResultSet> asyncQuery(Statement statement, String sql) throws SQLException {
        Future<ResultSet> future = query(statement);
        executeSilence(statement, sql);
        return future;
    }

    /**
     * 标记Statement的执行结果以异步方式返回, 并不真正的执行.
     * @param statement 待标记对象
     * @return Future对象, 可get()获取结果, 也可以注册回调方法.
     * @throws SQLException
     */
    public static Future<ResultSet> query(Statement statement) throws SQLException {
        return AsyncStatementInterceptor.intercept(statement, new ResultSetListener(statement));
    }
    /**
     * 标记Statement的执行结果以异步方式返回, 并不真正的执行.
     * @param statement 待标记对象
     * @return Future对象, 可get()获取结果, 也可以注册回调方法.
     * @throws SQLException
     */
    public static Future<Integer> update(Statement statement) throws SQLException {
        return AsyncStatementInterceptor.intercept(statement, new UpdateCountListener());
    }

    private static void executeSilence(Statement statement, String sql) throws SQLException {
        if (statement instanceof PreparedStatement) {
            if (sql != null) {
                if (statement.execute(sql)) {
                    statement.getResultSet().close();
                }
            } else {
                PreparedStatement preparedStatement = (PreparedStatement) statement;
                if (preparedStatement.execute()) {
                    preparedStatement.getResultSet().close();
                }
            }
        } else {
            if (sql != null) {
                if (statement.execute(sql)) {
                    statement.getResultSet().close();
                }
            } else {
                throw new RuntimeException("sql为空");
            }
        }
    }
}

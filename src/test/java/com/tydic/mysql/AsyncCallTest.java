package com.tydic.mysql;

import io.netty.util.concurrent.Future;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

/**
 * AsyncCall Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>九月 21, 2017</pre>
 */
public class AsyncCallTest {

    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {
    }

    /**
     * Method: execute(Statement statement, String sql)
     */
    @Test
    public void testExecute() throws Exception {
//TODO: Test goes here...

        final String url = AsyncCall.enable("jdbc:mysql://172.168.1.215:3307/vds");
        final Connection connection = DriverManager.getConnection(url, "root", "tydic");
        final PreparedStatement preparedStatement = connection.prepareStatement("select 'hello async-mysql-connector!'");
        AsyncCall.asyncQuery(preparedStatement).addListener(new FutureListener<ResultSet>() {
            public void operationComplete(Future<ResultSet> future) throws Exception {
                ResultSet resultSet = future.get();
                while (resultSet.next()) {
                    System.out.println(Thread.currentThread().getName() + " : " + resultSet.getString(1));
                }
                resultSet.close();
                preparedStatement.close();
            }
        }).sync();


        selectFromDual(connection);
        dropTable(connection);
        createTable(connection);
        insertTable(connection);
        selectTable(connection);
        connection.close();

    }

    protected void selectTable(Connection connection) throws SQLException, InterruptedException, java.util.concurrent.ExecutionException {
        Statement statement = connection.createStatement();
        Future<ResultSet> future = AsyncCall.asyncQuery(statement, "select * from T1");
        ResultSet resultSet = future.get();

        while (resultSet.next()) {
            System.out.println(resultSet.getString(1));
        }
        resultSet.close();
        statement.close();
    }

    private void insertTable(Connection connection) throws SQLException, InterruptedException, java.util.concurrent.ExecutionException {
        PreparedStatement statement = connection.prepareStatement("insert into T1 values (?, ?, ?)");
        statement.setInt(1, 1);
        statement.setInt(2, 1);
        statement.setInt(3, 1);
        Integer integer = AsyncCall.asyncUpdate(statement).get();
        System.out.println("UpdateCount : " + integer);
        statement.close();
    }

    private void createTable(Connection connection) throws SQLException, InterruptedException, java.util.concurrent.ExecutionException {
        Statement statement = connection.createStatement();
        Future<Integer> future = AsyncCall.asyncUpdate(statement, "CREATE TABLE IF NOT EXISTS T1 (a int, b int, c int, PRIMARY KEY(a))");
        Integer integer = future.get();
        System.out.println("UpdateCount : " + integer);
        statement.close();
    }

    private void dropTable(Connection connection) throws SQLException, InterruptedException, java.util.concurrent.ExecutionException {
        Statement statement = connection.createStatement();
        Integer integer = AsyncCall.asyncUpdate(statement, "DROP TABLE IF EXISTS T1").get();
        System.out.println("UpdateCount : " + integer);
        statement.close();
    }

    private void selectFromDual(Connection connection) throws SQLException, InterruptedException, java.util.concurrent.ExecutionException {
        Statement statement = connection.createStatement();
        ResultSet resultSet = AsyncCall.asyncQuery(statement, "select 'hello async-mysql-connector'").get();
        while (resultSet.next()) {
            System.out.println(resultSet.getString(1));
        }
        resultSet.close();
        statement.close();
    }


} 

# async-mysql-connector
异步的mysql驱动

```
<dependency>
  <groupId>com.github.bj-tydic</groupId>
  <artifactId>async-mysql-connector</artifactId>
  <version>1.0</version>
</dependency>
```

* 启用异步
``` java
final String url = AsyncCall.enable("jdbc:mysql://localhost:3306/async-mysql");
final Connection connection = DriverManager.getConnection(url, "root", "tydic");
```
   `AsyncCall.enable()`方法会向url中添加以下信息： 1. 禁
  用SSL， 2. 设置socketFactory， 3. 设置Statement拦截器

* 异步回调
```java
final String url = AsyncCall.enable("jdbc:mysql://localhost:3306/async-mysql");
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
```

* 异步执行
``` java
PreparedStatement statement = connection.prepareStatement("insert into T1 values (?, ?, ?)");
        statement.setInt(1, 1);
        statement.setInt(2, 1);
        statement.setInt(3, 1);
Integer integer = AsyncCall.asyncUpdate(statement).get();
```


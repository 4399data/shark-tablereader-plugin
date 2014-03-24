可以通过tblproperties中定义的shark.table.reader.class来获取不同的tablereader, 从而得到不同的rdd

1. JdbcFlagTableReader

  shark.table.reader.class = shark.execution.JdbcFlagTableReader

  在tblproperties中指定好查询的sql，同时支持从配置中获取参数值，替换sql中预定的标签。

  允许的属性：
  1. mapred.jdbc.driver.class - jdbc的driver类，该类所在的jar包必须在shark安装目录的lib目录下
  2. mapred.jdbc.url - jdbc使用的url，得包含账号密码
  3. mapred.jdbc.sql.key - 查询sql获取最小/大的列名
  4. mapred.jdbc.sql.template - 查询使用到的sql模板，只能包含from子句和where子句


  mapred.jdbc.sql.template中允许的标签：
  * {date} - Ymd格式的日期
  * {plat} - 平台标识
  * {timestart} - 日期对应的开始时间戳(秒)
  * {timeend} - 日期对应的结束时间戳(秒)

  eg:
  建表
<pre>
<code>
  create external table xxoolog(
    recdate string,
    rectime int,
    user_name string,
    sex int
  )
  stored by 'org.wso2.carbon.hadoop.hive.jdbc.storage.JDBCStorageHandler'
  tblproperties (
    'shark.table.reader.class' = 'shark.execution.JdbcFlagTableReader',
    'mapred.jdbc.driver.class' = 'com.mysql.jdbc.Driver',
    'mapred.jdbc.url' = 'jdbc:mysql://localhost/test?user=root&password=',
    'mapred.jdbc.sql.key' = 'rectime',
    'mapred.jdbc.sql.template' = 'test.xxoolog_{date}'
  )
</code>
</pre>
  查询
<pre>
<code>
  set mapred.jdbc.sql.args.date = 20140324;
  // 如果多个表查询，set mapred.jdbc.sql.args.default.xxoolog.date = 20140324;来控制某一张表的参数
  select * from xxoolog limit 10
</code>
</pre>
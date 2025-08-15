# hive metastore (HMS) - stores the table schema / metadata
# hive server2 - is a service that enables the clinets to execute queries against hive
# JDBC client - Beeline (to interact with hiverserver2)

# one way to connect to hive type - hive in terminal
# other way is type beeline in  a new terminal, type this one: !connect jdbc:hive2://m02.itversity.com:10000/;auth=noSasl

# 0: jdbc:hive2://m02.itversity.com:10000/> create database misgaurav_101;
# ERROR : FAILED: Execution Error, return code 1 from org.apache.hadoop.hive.ql.exec.DDLTask. MetaException(message:Got exception: org.apache.hadoop.security.AccessControlException
# Permission denied: user=itv020752, access=WRITE, inode="/user/hive/warehouse":hive:students:drwxr-xr-x

# where exactly your data kept in hdfs. if you keep your table in hive, where your data will reside in hdfs
# 0: jdbc:hive2://m02.itversity.com:10000/> set hive.metastore.warehouse.dir=/user/itv020752/warehouse;
# No rows affected (0.002 seconds)

# 0: jdbc:hive2://m02.itversity.com:10000/> create database misgaurav_101;
# No rows affected (0.061 seconds)
# 0: jdbc:hive2://m02.itversity.com:/10000/> use misgaurav_101;
# No rows affected (0.022 seconds)

# hive> use misgaurav_101;
# OK
# Time taken: 0.455 seconds
# hive> show tables;
# OK
# demo_table_1
# Time taken: 1.551 seconds, Fetched: 1 row(s)

# 0: jdbc:hive2://m02.itversity.com:10000/> show tables;
# +---------------+
# |   tab_name    |
# +---------------+
# | demo_table_1  |
# +---------------+
# 1 row selected (0.164 seconds)

# is your data stored as a table - No, your data store seperately (HDFS), your meta data will store seperatley (mysql)

# 0: jdbc:hive2://m02.itversity.com:10000/> select * from demo_table_1;
# +------------------+--------------------+-------------------+
# | demo_table_1.id  | demo_table_1.name  | demo_table_1.age  |
# +------------------+--------------------+-------------------+
# | 1                | John               | 25                |
# | 2                | Jane               | 30                |
# | 3                | Bob                | 22                |
# +------------------+--------------------+-------------------+
# 3 rows selected (22.711 seconds)

# hive> select * from demo_table_1;
# OK
# 1       John    25
# 2       Jane    30
# 3       Bob     22
# Time taken: 28.896 seconds, Fetched: 3 row(s)

# [itv020752@g01 ~]$ hadoop fs -ls /user/itv020752/warehouse/
# Found 6 items
# drwxr-xr-x   - itv020752 supergroup          0 2025-07-17 10:05 /user/itv020752/warehouse/itv020752_db.db
# drwxr-xr-x   - itv020752 supergroup          0 2025-07-17 10:15 /user/itv020752/warehouse/itv020752_partitioning.db
# drwxr-xr-x   - itv020752 supergroup          0 2025-08-15 00:48 /user/itv020752/warehouse/misgaurav_101.db
# drwxr-xr-x   - itv020752 supergroup          0 2025-08-14 09:43 /user/itv020752/warehouse/misgaurav_acid.db
# drwxr-xr-x   - itv020752 supergroup          0 2025-08-13 06:47 /user/itv020752/warehouse/misgaurav_hive.db
# drwxr-xr-x   - itv020752 supergroup          0 2025-08-13 07:17 /user/itv020752/warehouse/misgaurav_hive_new.db

# [itv020752@g01 ~]$ hadoop fs -cat /user/itv020752/warehouse/misgaurav_101.db/demo_table_1/000000_0
# 1John25
# 2Jane30
# 3Bob22

# table information will be stored in mysql, passwords is :itversity

# [itv020752@g01 ~]$ mysql -A -u retail_user -h ms.itversity.com -p
# Enter password: 
# Welcome to the MariaDB monitor.  Commands end with ; or \g.
# Your MariaDB connection id is 16011
# Server version: 5.5.68-MariaDB MariaDB Server

# Copyright (c) 2000, 2018, Oracle, MariaDB Corporation Ab and others.

# Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

# since, we don't have an access to hive directory, otherwise we would be able to see metastore, which is a db, in the below tables as well:

# MariaDB [(none)]> show databases;
# +--------------------+
# | Database           |
# +--------------------+
# | information_schema |
# | retail_db          |
# | retail_export      |
# +--------------------+
# 3 rows in set (0.01 sec)

# if we have a permission as a hive user, then we need to use the below commands, then we'll see metastore, which is a db, after passing the credentials:
# [itv020752@g01 ~]$ mysql -u hive -h gw01.itversity.com -p

# internal table definitions:
# 0: jdbc:hive2://m02.itversity.com:10000/> drop table demo_table_1;
# No rows affected (0.291 seconds)
# [itv020752@g01 ~]$ hadoop fs -ls /user/itv020752/warehouse/misgaurav_101.db/; returns blank
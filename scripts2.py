# it's your directory where temporary if required, your hive data will be stored.
# <property>
#     <name>hive.exec.local.scratchdir</name>
#     <value>/tmp/${user.name}</value>
#     <description>Local scratch space for Hive jobs</description>
#   </property>

# how to set up a MYSQL in hive
# this part is pending (under MYSQL, we'll be able to see hive metastore database, and all the metastore info about the db or tables will stored here)

# hive> set hive.execution.engine;
# hive.execution.engine=mr
# hive> create database misgaurav_dummy;
# this will create a database under /user/hive/warehouse in hdfs; but for this lab we don't have an aceess to write it here.
# Execution Error, return code 1 from org.apache.hadoop.hive.ql.exec.DDLTask. MetaException(message:Got exception: org.apache.hadoop.
# security.AccessControlException Permission denied: user=itv020752, access=WRITE, inode="/user/hive/warehouse":hive:students:drwxr-xr-x
# the default location of any db or table created in Hive would be /user/hive/warehouse.


# if we want to change the default directory, then use the below one:
# hive> create database misgaurav_temp location '/user/hive/temp';
# this will create a database under /user/hive/temp in hdfs; but for this lab we don't have an aceess to write it here.
# FAILED: Execution Error, return code 1 from org.apache.hadoop.hive.ql.exec.DDLTask. MetaException(message:Got exception: org.apache.hadoop.
# security.AccessControlException Permission denied: user=itv020752, access=WRITE, inode="/user/hive":hdfs:supergroup:drwxr-xr-x


# hive> create table misgaurav_orders 
#     > (order_id string, order_date string, customer_id string, order_status string)
#     > row format delimited 
#     > fields terminated by ','
#     > stored as textfile;
# FAILED: Execution Error, return code 1 from org.apache.hadoop.hive.ql.exec.DDLTask. MetaException(message:Got exception: org.apache.hadoop.
# security.AccessControlException Permission denied: user=itv020752, access=WRITE, inode="/user":hdfs:supergroup:drwxr-xr-x

# when we create a table/db by using spark.sql, by default it will create a table in hive, and the data will be stored in hdfs.
# the location of the data will be /user/<user_name>/warehouse/<db_name>.db/<table_name>/, for this lab it will be /user/itv020752/warehouse/misgaurav_hive.db/misgaurav_orders/

# if we want to create a db/table by using hive cli, then we need to change the default location of the hive metastore directory.
# hive> set hive.metastore.warehouse.dir=/user/itv020752/warehouse;
# No rows affected (0.002 seconds)
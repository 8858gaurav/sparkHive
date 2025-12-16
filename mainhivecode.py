# type hive in terminal
# if we type create database misgaurav_hive_part1, it'll give me an error

# FAILED: Execution Error, return code 1 from org.apache.hadoop.hive.ql.exec.DDLTask. Database misgaurav_hive_new already exists
# hive> create database misgaurav_hive_part1;
# FAILED: Execution Error, return code 1 from org.apache.hadoop.hive.ql.exec.DDLTask. MetaException(message:Got exception: org.apache.hadoop.security.AccessControlException Permission denied: user=itv020752, access=WRITE, inode="/user/hive/warehouse":hive:students:drwxr-xr-x
                                                                                                  
# hive> set hive.metastore.warehouse.dir = /user/itv020752/warehouse;
                                                                                                  
# hive> create database misgaurav_hive_part1; # it'll work then        

import pyspark, pandas as pd
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import getpass, time
username = getpass.getuser()
print(username)

spark = SparkSession \
           .builder \
           .appName("basic application") \
           .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse") \
           .enableHiveSupport() \
           .master("yarn") \
           .getOrCreate()
spark.sparkContext.setLogLevel('WARN')

import re
f = []
for i in spark.catalog.listDatabases():
    if re.search(r'misgaurav_hive_part1', i.name):
        f.append(i)
        
print(f)
# [Database(name='misgaurav_hive_part1', description='', locationUri='hdfs://m01.itversity.com:9000/user/itv020752/warehouse/misgaurav_hive_part1.db')]

print(spark.catalog.listDatabases()[0:2])
# [Database(name='0000000000000_msdian', description='', locationUri='hdfs://m01.itversity.com:9000/user/itv009768/warehouse/0000000000000_msdian.db'),
# Database(name='0000000000000_naveen_db', description='', locationUri='hdfs://m01.itversity.com:9000/user/itv009240/warehouse/0000000000000_naveen_db.db')]

# !hadoop fs -ls /user/itv020752/warehouse/
# rwxr-xr-x   - itv020752 supergroup          0 2025-11-13 12:53 /user/itv020752/warehouse/itv020752_db.db
# drwxr-xr-x   - itv020752 supergroup          0 2025-11-11 09:10 /user/itv020752/warehouse/itv020752_db_new.db
# drwxr-xr-x   - itv020752 supergroup          0 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db
# drwxr-xr-x   - itv020752 supergroup          0 2025-07-17 10:15 /user/itv020752/warehouse/itv020752_partitioning.db
# drwxr-xr-x   - itv020752 supergroup          0 2025-08-15 01:23 /user/itv020752/warehouse/misgaurav_101.db
# drwxr-xr-x   - itv020752 supergroup          0 2025-08-14 09:43 /user/itv020752/warehouse/misgaurav_acid.db
# drwxr-xr-x   - itv020752 supergroup          0 2025-08-13 06:47 /user/itv020752/warehouse/misgaurav_hive.db
# drwxr-xr-x   - itv020752 supergroup          0 2025-08-13 07:17 /user/itv020752/warehouse/misgaurav_hive_new.db
# drwxr-xr-x   - itv020752 supergroup          0 2025-12-16 05:39 /user/itv020752/warehouse/misgaurav_hive_part1.db

# hive> CREATE TABLE misgaurav_hive_part1.misgaurav_hive_table_part1 (
#     >     order_id STRING, 
#     >     order_date STRING, 
#     >     customer_id STRING, 
#     >     order_status STRING
#     > )
#     > ;
# OK

import re
g = []
for i in spark.catalog.listTables("misgaurav_hive_part1"):
    if re.search(r'misgaurav_hive_table_part1', i.name):
        g.append(i)
        
print(g)
# [Table(name='misgaurav_hive_table_part1', database='misgaurav_hive_part1', description=None, tableType='MANAGED', isTemporary=False)]

# hive> set hive.execution.engine;
# hive.execution.engine=mr

# hive> set hive.execution.engine = spark;
# hive> set hive.execution.engine;
# hive.execution.engine=spark

# hive> set hive.execution.engine = tez;
# hive> set hive.execution.engine;
# hive.execution.engine=tez

# [itv020752@g01 ~]$ grep -i -n "hive.metastore.warehouse.dir" /opt/apache-hive-3.1.2-bin/conf/hive-site.xml 
# 451:    <name>hive.metastore.warehouse.dir</name>

# [itv020752@g01 ~]$ sed -n '452p' /opt/apache-hive-3.1.2-bin/conf/hive-site.xml
#     <value>/user/hive/warehouse</value>
    
# [itv020752@g01 ~]$ grep -i -n "hive.metastore.db.type" /opt/apache-hive-3.1.2-bin/conf/hive-site.xml 
# 443:    <name>hive.metastore.db.type</name>

# [itv020752@g01 ~]$ sed -n '444p' /opt/apache-hive-3.1.2-bin/conf/hive-site.xml
#     <value>postgres</value>

# [itv020752@g01 ~]$ grep -i -n "javax.jdo.option.ConnectionUserName" /opt/apache-hive-3.1.2-bin/conf/hive-site.xml 
# 1125:    <name>javax.jdo.option.ConnectionUserName</name>

# [itv020752@g01 ~]$ sed -n '1126p' /opt/apache-hive-3.1.2-bin/conf/hive-site.xml
#     <value>hive</value>

# [itv020752@g01 ~]$ grep -i -n "javax.jdo.option.ConnectionPassword" /opt/apache-hive-3.1.2-bin/conf/hive-site.xml 
# 6769:    <value>javax.jdo.option.ConnectionPassword,hive.server2.keystore.password,fs.s3.awsAccessKeyId,fs.s3.awsSecretAccessKey,fs.s3n.awsAccessKeyId,fs.s3n.awsSecretAccessKey,fs.s3a.access.key,fs.s3a.secret.key,fs.s3a.proxy.password,dfs.adls.oauth2.credential,fs.adl.oauth2.credential</value>

# [itv020752@g01 ~]$ sed -n '6770p' /opt/apache-hive-3.1.2-bin/conf/hive-site.xml
#     <description>Comma separated list of configuration options which should not be read by normal user like passwords</description>
    
# [itv020752@g01 ~]$ grep -i -n "hive.exec.local" /opt/apache-hive-3.1.2-bin/conf/hive-site.xml
# 142:    <name>hive.exec.local.scratchdir</name>

# [itv020752@g01 ~]$ sed -n '143p' /opt/apache-hive-3.1.2-bin/conf/hive-site.xml
#     <value>/tmp/${user.name}</value>


!hadoop fs -ls /user/itv020752/warehouse/misgaurav_hive_part1.db
Found 1 items
drwxr-xr-x   - itv020752 supergroup          0 2025-12-16 05:58 /user/itv020752/warehouse/misgaurav_hive_part1.db/misgaurav_hive_table_part1

# hive> create database misgaurav_temp location '/user/itv020752/hive/mytemp';
# OK
# Time taken: 0.143 seconds
    
# !hadoop fs -ls /user/itv020752/hive
# Found 1 items
# drwxr-xr-x   - itv020752 supergroup          0 2025-12-16 06:58 /user/itv020752/hive/mytemp

# hive> use misgaurav_temp;
# OK

# hive> CREATE TABLE misgaurav_temp.misgaurav_hive_table_part1 (
#     > order_id STRING, 
#     > order_date STRING, 
#     > customer_id STRING, 
#     > order_status STRING
#     > );
# OK
# Time taken: 1.346 seconds

!hadoop fs -ls /user/itv020752/hive/mytemp/
# Found 1 items
# drwxr-xr-x   - itv020752 supergroup          0 2025-12-16 07:10 /user/itv020752/hive/mytemp/misgaurav_hive_table_part1

!hadoop fs -ls /user/itv020752/warehouse/ # you'll not see misgaurav_temp under here
# Found 9 items
# drwxr-xr-x   - itv020752 supergroup          0 2025-11-13 12:53 /user/itv020752/warehouse/itv020752_db.db
# drwxr-xr-x   - itv020752 supergroup          0 2025-11-11 09:10 /user/itv020752/warehouse/itv020752_db_new.db
# drwxr-xr-x   - itv020752 supergroup          0 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db
# drwxr-xr-x   - itv020752 supergroup          0 2025-07-17 10:15 /user/itv020752/warehouse/itv020752_partitioning.db
# drwxr-xr-x   - itv020752 supergroup          0 2025-08-15 01:23 /user/itv020752/warehouse/misgaurav_101.db
# drwxr-xr-x   - itv020752 supergroup          0 2025-08-14 09:43 /user/itv020752/warehouse/misgaurav_acid.db
# drwxr-xr-x   - itv020752 supergroup          0 2025-08-13 06:47 /user/itv020752/warehouse/misgaurav_hive.db
# drwxr-xr-x   - itv020752 supergroup          0 2025-08-13 07:17 /user/itv020752/warehouse/misgaurav_hive_new.db
# drwxr-xr-x   - itv020752 supergroup          0 2025-12-16 05:58 /user/itv020752/warehouse/misgaurav_hive_part1.db

# hive> use misgaurav_hive_part1;
# OK
# Time taken: 0.822 seconds
# to see the current database name in hive prompt;
# hive> set hive.cli.print.current.db = true;
# hive (misgaurav_hive_part1)> 

# to see the column headers while selecting data from hive table;
# hive (misgaurav_hive_part1)> set hive.cli.print.header=true;
# hive (misgaurav_hive_part1)> select * from misgaurav_hive_table_part1;
# OK
# misgaurav_hive_table_part1.order_id     misgaurav_hive_table_part1.order_date   misgaurav_hive_table_part1.customer_id  misgaurav_hive_table_part1.order_status
# Time taken: 49.832 seconds

# hive> use misgaurav_hive_part1;
# OK
# Time taken: 0.602 seconds
# hive> show tables;
# OK
# misgaurav_hive_table_part1
# Time taken: 1.527 seconds, Fetched: 1 row(s)
# hive> select * from misgaurav_hive_table_part1;
# OK
# Time taken: 31.852 seconds
hive> create database if not exists misgaurav_hive;
FAILED: Execution Error, return code 1 from org.apache.hadoop.hive.ql.exec.DDLTask. MetaException(message:Got exception: org.apache.hadoop.security.AccessControlException Permission denied: user=itv020752, access=WRITE, inode="/user/hive/warehouse":hive:students:drwxr-xr-x

hive> set hive.metastore.warehouse.dir = /user/itv020752/warehouse/;
hive> create database if not exists misgaurav_hive;
OK
Time taken: 0.343 seconds

hive> CREATE EXTERNAL TABLE IF NOT EXISTS misgaurav_hive.external_customer_table (
    >customer_id STRING,
    >customer_fname STRING,
    >customer_lname STRING,
    >username STRING,
    >password STRING,
    >address STRING,
    >city STRING,
    >state STRING,
    >pincode STRING
>) 
>ROW FORMAT DELIMITED
>FIELDS TERMINATED BY ','
>STORED AS textfile
>LOCATION '/user/itv020752/hive_datasets/customers';
OK
Time taken: 0.300 seconds


hive> CREATE EXTERNAL TABLE IF NOT EXISTS misgaurav_hive.external_orders_table (
    >order_id STRING,
    >order_date STRING,
    >customer_id STRING,
    >order_status STRING
>) 
>ROW FORMAT DELIMITED
>FIELDS TERMINATED BY ','
>STORED AS textfile
>LOCATION '/user/itv020752/hive_datasets/orders';
OK
Time taken: 0.323 seconds

hive> show tables;
OK
external_customer_table
external_orders_table
Time taken: 0.026 seconds, Fetched: 2 row(s)

[itv020752@g01 ~]$ ls -l -h hive_datasets/
total 3.8M
-rw-r--r-- 1 itv020752 students 932K Dec 18 01:52 customers.csv
-rw-r--r-- 1 itv020752 students 2.9M Dec 18 02:00 orders.csv

import os
print(int(os.path.getsize('hive_datasets/orders.csv'))/1024/1024) ## size in mb on local machine --> 2.86
print(int(os.path.getsize('hive_datasets/customers.csv'))/1024/1024) ## size in mb on local machine --> 0.90

import pyspark, pandas as pd
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import getpass, time
username = getpass.getuser()
print(username)

spark = SparkSession \
           .builder \
           .appName("Yoyo application") \
           .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse") \
           .enableHiveSupport() \
           .master("yarn") \
           .getOrCreate()
spark.sparkContext.setLogLevel('WARN')

for k, v in spark.sparkContext.getConf().getAll():
    print(f"{k} = {v}")
# spark.eventLog.enabled = true
# spark.org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter.param.PROXY_URI_BASES = http://m02.itversity.com:19088/proxy/application_1766025126719_0105
# spark.sql.repl.eagerEval.enabled = true
# spark.eventLog.dir = hdfs:///spark-logs
# spark.driver.appUIAddress = http://g01.itversity.com:4040
# spark.dynamicAllocation.maxExecutors = 10
# spark.driver.port = 37345
# spark.sql.warehouse.dir = /user/itv020752/warehouse
# spark.yarn.historyServer.address = m02.itversity.com:18080
# spark.app.name = Yoyo application
# spark.executorEnv.PYTHONPATH = /opt/spark-3.1.2-bin-hadoop3.2/python/lib/py4j-0.10.9-src.zip:/opt/spark-3.1.2-bin-hadoop3.2/python/<CPS>{{PWD}}/pyspark.zip<CPS>{{PWD}}/py4j-0.10.9-src.zip
# spark.yarn.jars = 
# spark.history.provider = org.apache.spark.deploy.history.FsHistoryProvider
# spark.serializer.objectStreamReset = 100
# spark.history.fs.logDirectory = hdfs:///spark-logs
# spark.submit.deployMode = client
# spark.history.fs.update.interval = 10s
# spark.driver.extraJavaOptions = -Dderby.system.home=/tmp/derby/
# spark.org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter.param.PROXY_HOSTS = m02.itversity.com
# spark.ui.filters = org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter
# spark.executor.extraLibraryPath = /opt/hadoop/lib/native
# spark.ui.proxyBase = /proxy/application_1766025126719_0105
# spark.app.id = application_1766025126719_0105
# spark.history.ui.port = 18080
# spark.shuffle.service.enabled = true
# spark.dynamicAllocation.minExecutors = 2
# spark.executor.id = driver
# spark.driver.host = g01.itversity.com
# spark.history.fs.cleaner.enabled = true
# spark.master = yarn
# spark.sql.catalogImplementation = hive
# spark.rdd.compress = True
# spark.submit.pyFiles = 
# spark.yarn.isPython = true
# spark.dynamicAllocation.enabled = true
# spark.app.startTime = 1766041604140
# spark.ui.showConsoleProgress = true

import re
f = []
for i in spark.catalog.listTables("misgaurav_hive"):
    if re.search(r'external', i.name):
        f.append(i)

g = []
for i in spark.catalog.listDatabases():
    if re.search(r'misgaurav_hive', i.name):
        g.append(i)

print(g, f)
# [Database(name='misgaurav_hive', description='', locationUri='hdfs://m01.itversity.com:9000/user/itv020752/warehouse/misgaurav_hive.db'), 
# Database(name='misgaurav_hive_new', description='', locationUri='hdfs://m01.itversity.com:9000/user/itv020752/warehouse/misgaurav_hive_new.db'), 
# Database(name='misgaurav_hive_part1', description='', locationUri='hdfs://m01.itversity.com:9000/user/itv020752/warehouse/misgaurav_hive_part1.db')] 
# [Table(name='external_customer_table', database='misgaurav_hive', description=None, tableType='EXTERNAL', isTemporary=False), 
# Table(name='external_orders_table', database='misgaurav_hive', description=None, tableType='EXTERNAL', isTemporary=False)]

!hadoop fs -ls /user/itv020752/warehouse/
# Found 9 items
# drwxr-xr-x   - itv020752 supergroup          0 2025-11-13 12:53 /user/itv020752/warehouse/itv020752_db.db
# drwxr-xr-x   - itv020752 supergroup          0 2025-11-11 09:10 /user/itv020752/warehouse/itv020752_db_new.db
# drwxr-xr-x   - itv020752 supergroup          0 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db
# drwxr-xr-x   - itv020752 supergroup          0 2025-07-17 10:15 /user/itv020752/warehouse/itv020752_partitioning.db
# drwxr-xr-x   - itv020752 supergroup          0 2025-08-15 01:23 /user/itv020752/warehouse/misgaurav_101.db
# drwxr-xr-x   - itv020752 supergroup          0 2025-08-14 09:43 /user/itv020752/warehouse/misgaurav_acid.db
# drwxr-xr-x   - itv020752 supergroup          0 2025-12-18 01:45 /user/itv020752/warehouse/misgaurav_hive.db
# drwxr-xr-x   - itv020752 supergroup          0 2025-08-13 07:17 /user/itv020752/warehouse/misgaurav_hive_new.db
# drwxr-xr-x   - itv020752 supergroup          0 2025-12-16 05:58 /user/itv020752/warehouse/misgaurav_hive_part1.db


hive> select O.*, C.* from external_orders_table O inner join external_customer_table C
    > on O.customer_id = C.customer_id limit 5;
Query ID = itv020752_20251218022108_00836849-377e-4e7a-9625-6a221c7f0bdf
Total jobs = 1
SLF4J: Found binding in [jar:file:/opt/apache-hive-3.1.2-bin/lib/log4j-slf4j-impl-2.10.0.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/opt/hadoop-3.3.0/share/hadoop/common/lib/slf4j-log4j12-1.7.25.jar!/org/slf4j/impl/StaticLoggerBinder.cla
ss]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.SLF4J: Actual binding is of type [org.apache.logging.slf4j.
Log4jLoggerFactory]

2025-12-18 02:21:57     End of local task; Time Taken: 0.772 sec.
Execution completed successfully
MapredLocal task succeeded =====> see this one
Launching Job 1 out of 1
Number of reduce tasks is set to 0 since there's no reduce operator
Starting Job = job_1766025126719_0112, Tracking URL = http://m02.itversity.com:19088/proxy/application_1766025126719_0112/
Kill Command = /opt/hadoop/bin/mapred job  -kill job_1766025126719_0112
Hadoop job information for Stage-3: number of mappers: 0; number of reducers: 0 =====> see this one
2025-12-18 02:22:07,819 Stage-3 map = 0%,  reduce = 0%
Ended Job = job_1766025126719_0112
MapReduce Jobs Launched:  ========> see this one
Stage-Stage-3:  HDFS Read: 0 HDFS Write: 0 SUCCESS
Total MapReduce CPU Time Spent: 0 msec
OK
Time taken: 61.595 seconds
hive> create database if not exists misgaurav_hive;
FAILED: Execution Error, return code 1 from org.apache.hadoop.hive.ql.exec.DDLTask. MetaException(message:Got exception: org.apache.hadoop.security.AccessControlException Permission denied: user=itv020752, access=WRITE, inode="/user/hive/warehouse":hive:students:drwxr-xr-x

hive> set hive.metastore.warehouse.dir = /user/itv020752/warehouse/;
hive> create database if not exists misgaurav_hive;
OK
Time taken: 0.343 seconds 

[itv020752@g01 ~]$ ls -l -h hive_datasets/
total 3.8M
-rw-r--r-- 1 itv020752 students 932K Dec 18 01:52 customers.csv
-rw-r--r-- 1 itv020752 students 2.9M Dec 18 02:00 orders.csv

import os
print(int(os.path.getsize('hive_datasets/orders.csv'))/1024/1024) ## size in mb on local machine --> 2.86
print(int(os.path.getsize('hive_datasets/customers.csv'))/1024/1024) ## size in mb on local machine --> 0.90

!hadoop fs -put /home/itv020752/hive_datasets/customers.csv /user/itv020752/hive_datasets/customers
!hadoop fs -put /home/itv020752/hive_datasets/orders.csv /user/itv020752/hive_datasets/orders

!hadoop fs -ls /user/itv020752/hive_datasets/orders
# Found 1 items
# -rw-r--r--   3 itv020752 supergroup    2999944 2025-12-18 12:06 /user/itv020752/hive_datasets/orders/orders.csv

!hadoop fs -ls /user/itv020752/hive_datasets/customers
# Found 1 items
# -rw-r--r--   3 itv020752 supergroup     953719 2025-12-18 12:06 /user/itv020752/hive_datasets/customers/customers.csv

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

hive (default)> use misgaurav_hive;
OK
Time taken: 0.064 seconds
hive (misgaurav_hive)> set hive.cli.print.current.db = true;

# No hash table will be created, cuz map side join is disabled by default in Hive.
hive (misgaurav_hive)> select O.*, C.* from external_orders_table O inner join external_customer_table C                     >     on O.customer_id = C.customer_id limit 5;
Query ID = itv020752_20251218122801_afafe626-ef32-4287-bf00-94ecfe21389e
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1766025126719_0338, Tracking URL = http://m02.itversity.com:19088/proxy/application_1766025126719_0338/
Kill Command = /opt/hadoop/bin/mapred job  -kill job_1766025126719_0338
Hadoop job information for Stage-1: number of mappers: 2; number of reducers: 1
2025-12-18 12:29:13,672 Stage-1 map = 0%,  reduce = 0%
2025-12-18 12:29:17,852 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 7.07 sec
2025-12-18 12:29:22,000 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 9.05 sec
MapReduce Total cumulative CPU time: 9 seconds 50 msec
Ended Job = job_1766025126719_0338
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 2  Reduce: 1   Cumulative CPU: 9.05 sec   HDFS Read: 3977326 HDFS Write: 738 SUCCESS
Total MapReduce CPU Time Spent: 9 seconds 50 msec
OK
22945   2013-12-13 00:00:00.0   1       COMPLETE        1       Richard Hernandez       XXXXXXXXX       XXXXXXXXX       6303 Heather PlazaB
rownsville      TX      78521
45239   2014-05-01 00:00:00.0   10      COMPLETE        10      Melissa Smith   XXXXXXXXX       XXXXXXXXX       8598 Harvest Beacon Plaza S
tafford VA      22554
56133   2014-07-15 00:00:00.0   10      COMPLETE        10      Melissa Smith   XXXXXXXXX       XXXXXXXXX       8598 Harvest Beacon Plaza S
tafford VA      22554
22395   2013-12-09 00:00:00.0   100     CANCELED        100     George  Barrett XXXXXXXXX       XXXXXXXXX       4110 Silent Pointe      Cag
uas     PR      00725
54995   2014-07-08 00:00:00.0   100     COMPLETE        100     George  Barrett XXXXXXXXX       XXXXXXXXX       4110 Silent Pointe      Cag
uas     PR      00725
Time taken: 82.579 seconds, Fetched: 5 row(s)

# to show the current db in hive prompt
hive> set hive.cli.print.current.db = true;

# disbale the map side join
hive (misgaurav_hive)> set hive.auto.convert.join = false;

hive (misgaurav_hive)> set hive.mapjoin.smalltable.filesize;
hive.mapjoin.smalltable.filesize=25000000
## if table < 25 mb, then it will automatically trigger the broadcast or map side join, once we enable map side join.

#### you'll see Join Operator, not Map Join Operator, line 364  ##########

hive (misgaurav_hive)> explain extended select O.*, C.* from external_orders_table O inner join external_customer_table C
                     >                      on O.customer_id = C.customer_id limit 5;
OK
STAGE DEPENDENCIES:
  Stage-1 is a root stage
  Stage-0 depends on stages: Stage-1

STAGE PLANS:
  Stage: Stage-1
    Map Reduce
      Map Operator Tree:
          TableScan
            alias: o
            Statistics: Num rows: 51368 Data size: 29999440 Basic stats: COMPLETE Column stats: NONE
            GatherStats: false
            Filter Operator
              isSamplingPred: false
              predicate: customer_id is not null (type: boolean)
              Statistics: Num rows: 51368 Data size: 29999440 Basic stats: COMPLETE Column stats: NONE
              Select Operator
                expressions: order_id (type: string), order_date (type: string), customer_id (type: string), order_status (type: string)
                outputColumnNames: _col0, _col1, _col2, _col3
                Statistics: Num rows: 51368 Data size: 29999440 Basic stats: COMPLETE Column stats: NONE
                Reduce Output Operator
                  key expressions: _col2 (type: string)
                  null sort order: a
                  sort order: +
                  Map-reduce partition columns: _col2 (type: string)
                  Statistics: Num rows: 51368 Data size: 29999440 Basic stats: COMPLETE Column stats: NONE
                  tag: 0
                  value expressions: _col0 (type: string), _col1 (type: string), _col3 (type: string)
                  auto parallelism: false
          TableScan
            alias: c
            Statistics: Num rows: 8798 Data size: 9537190 Basic stats: COMPLETE Column stats: NONE
            GatherStats: false
            Filter Operator
              isSamplingPred: false
              predicate: customer_id is not null (type: boolean)
              Statistics: Num rows: 8798 Data size: 9537190 Basic stats: COMPLETE Column stats: NONE
              Select Operator
                expressions: customer_id (type: string), customer_fname (type: string), customer_lname (type: string), username (type: string), password (type: string), address (type: string), city (type: string), state (type: string), pincode (type: string)
                outputColumnNames: _col0, _col1, _col2, _col3, _col4, _col5, _col6, _col7, _col8
                Statistics: Num rows: 8798 Data size: 9537190 Basic stats: COMPLETE Column stats: NONE
                Reduce Output Operator
                  key expressions: _col0 (type: string)
                  null sort order: a
                  sort order: +
                  Map-reduce partition columns: _col0 (type: string)
                  Statistics: Num rows: 8798 Data size: 9537190 Basic stats: COMPLETE Column stats: NONE
                  tag: 1
                  value expressions: _col1 (type: string), _col2 (type: string), _col3 (type: string), _col4 (type: string), _col5 (type: string), _col6 (type: string), _col7 (type: string), _col8 (type: string)
                  auto parallelism: false
      Path -> Alias:
        hdfs://m01.itversity.com:9000/user/itv020752/hive_datasets/customers [$hdt$_1:c]
        hdfs://m01.itversity.com:9000/user/itv020752/hive_datasets/orders [$hdt$_0:o]
      Path -> Partition:
        hdfs://m01.itversity.com:9000/user/itv020752/hive_datasets/customers 
          Partition
            base file name: customers
            input format: org.apache.hadoop.mapred.TextInputFormat
            output format: org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
            properties:
              EXTERNAL TRUE
              bucket_count -1
              bucketing_version 2
              column.name.delimiter ,
              columns customer_id,customer_fname,customer_lname,username,password,address,city,state,pincode
              columns.comments 
              columns.types string:string:string:string:string:string:string:string:string
              field.delim ,
              file.inputformat org.apache.hadoop.mapred.TextInputFormat
              file.outputformat org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
              location hdfs://m01.itversity.com:9000/user/itv020752/hive_datasets/customers
              name misgaurav_hive.external_customer_table
              numFiles 1
              serialization.ddl struct external_customer_table { string customer_id, string customer_fname, string customer_lname, string username, string password, string address, string city, string state, string pincode}
              serialization.format ,
              serialization.lib org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
              totalSize 953719
              transient_lastDdlTime 1766078702
            serde: org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
          
              input format: org.apache.hadoop.mapred.TextInputFormat
              output format: org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
              properties:
                EXTERNAL TRUE
                bucket_count -1
                bucketing_version 2
                column.name.delimiter ,
                columns customer_id,customer_fname,customer_lname,username,password,address,city,state,pincode
                columns.comments 
                columns.types string:string:string:string:string:string:string:string:string
                field.delim ,
                file.inputformat org.apache.hadoop.mapred.TextInputFormat
                file.outputformat org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
                location hdfs://m01.itversity.com:9000/user/itv020752/hive_datasets/customers
                name misgaurav_hive.external_customer_table
                numFiles 1
                serialization.ddl struct external_customer_table { string customer_id, string customer_fname, string customer_lname, string username, string password, string address, string city, string state, string pincode}
                serialization.format ,
                serialization.lib org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
                totalSize 953719
                transient_lastDdlTime 1766078702
              serde: org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
              name: misgaurav_hive.external_customer_table
            name: misgaurav_hive.external_customer_table
        hdfs://m01.itversity.com:9000/user/itv020752/hive_datasets/orders 
          Partition
            base file name: orders
            input format: org.apache.hadoop.mapred.TextInputFormat
            output format: org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
            properties:
              EXTERNAL TRUE
              bucket_count -1
              bucketing_version 2
              column.name.delimiter ,
              columns order_id,order_date,customer_id,order_status
              columns.comments 
              columns.types string:string:string:string
              field.delim ,
              file.inputformat org.apache.hadoop.mapred.TextInputFormat
              file.outputformat org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
              location hdfs://m01.itversity.com:9000/user/itv020752/hive_datasets/orders
              name misgaurav_hive.external_orders_table
              serialization.ddl struct external_orders_table { string order_id, string order_date, string customer_id, string order_status}
              serialization.format ,
              serialization.lib org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
              transient_lastDdlTime 1766077857
            serde: org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
          
              input format: org.apache.hadoop.mapred.TextInputFormat
              output format: org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
              properties:
                EXTERNAL TRUE
                bucket_count -1
                bucketing_version 2
                column.name.delimiter ,
                columns order_id,order_date,customer_id,order_status
                columns.comments 
                columns.types string:string:string:string
                field.delim ,
                file.inputformat org.apache.hadoop.mapred.TextInputFormat
                file.outputformat org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
                location hdfs://m01.itversity.com:9000/user/itv020752/hive_datasets/orders
                name misgaurav_hive.external_orders_table
                serialization.ddl struct external_orders_table { string order_id, string order_date, string customer_id, string order_status}
                serialization.format ,
                serialization.lib org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
                transient_lastDdlTime 1766077857
              serde: org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
              name: misgaurav_hive.external_orders_table
            name: misgaurav_hive.external_orders_table
      Truncated Path -> Alias:
        hdfs://m01.itversity.com:9000/user/itv020752/hive_datasets/customers [$hdt$_1:c]
        hdfs://m01.itversity.com:9000/user/itv020752/hive_datasets/orders [$hdt$_0:o]
      Needs Tagging: true
      Reduce Operator Tree:
        Join Operator
          condition map:
               Inner Join 0 to 1
          keys:
            0 _col2 (type: string)
            1 _col0 (type: string)
          outputColumnNames: _col0, _col1, _col2, _col3, _col4, _col5, _col6, _col7, _col8, _col9, _col10, _col11, _col12
          Statistics: Num rows: 56504 Data size: 32999384 Basic stats: COMPLETE Column stats: NONE
          Limit
            Number of rows: 5
            Statistics: Num rows: 5 Data size: 2920 Basic stats: COMPLETE Column stats: NONE
            File Output Operator
              compressed: false
              GlobalTableId: 0
              directory: hdfs://m01.itversity.com:9000/tmp/hive/itv020752/itv020752/72fa5fea-f7de-48fc-921f-e3ac6a8771a3/hive_2025-12-18_12-31-29_260_4674909483033431033-2/-mr-10001/.hive-staging_hive_2025-12-18_12-31-29_260_4674909483033431033-2/-ext-10002
              NumFilesPerFileSink: 1
              Statistics: Num rows: 5 Data size: 2920 Basic stats: COMPLETE Column stats: NONE
              Stats Publishing Key Prefix: hdfs://m01.itversity.com:9000/tmp/hive/itv020752/itv020752/72fa5fea-f7de-48fc-921f-e3ac6a8771a3/hive_2025-12-18_12-31-29_260_4674909483033431033-2/-mr-10001/.hive-staging_hive_2025-12-18_12-31-29_260_4674909483033431033-2/-ext-10002/
              table:
                  input format: org.apache.hadoop.mapred.SequenceFileInputFormat
                  output format: org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat
                  properties:
                    columns _col0,_col1,_col2,_col3,_col4,_col5,_col6,_col7,_col8,_col9,_col10,_col11,_col12
                    columns.types string:string:string:string:string:string:string:string:string:string:string:string:string
                    escape.delim \
                    hive.serialization.extend.additional.nesting.levels true
                    serialization.escape.crlf true
                    serialization.format 1
                    serialization.lib org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
                  serde: org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
              TotalFiles: 1
              GatherStats: false
              MultiFileSpray: false

  Stage: Stage-0
    Fetch Operator
      limit: 5
      Processor Tree:
        ListSink

Time taken: 56.773 seconds, Fetched: 195 row(s)
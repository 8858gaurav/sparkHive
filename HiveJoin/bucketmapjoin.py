# here the table is very small, not big enough as expected for a bucket map join. 
# thus the join falls back to a common join. see line no : 433, & 574 in this file.

hive> use misgaurav_hive;
OK
Time taken: 4.341 seconds

hive> set hive.cli.print.current.db = true;
hive (misgaurav_hive)> 

hive (misgaurav_hive)> show tables;
OK
external_customer_table
external_orders_table
Time taken: 0.077 seconds, Fetched: 2 row(s)
hive (misgaurav_hive)> 

# both the table should be bucketed on the join columns
# a bucket of 1 table should be integral of bucket of other table, e.b table 1 has 4 buckets, and tables 2 has 8 buckets.

# use, when both the tables are larger in size, none of them can fit in memory.

hive (misgaurav_hive)> CREATE EXTERNAL TABLE IF NOT EXISTS misgaurav_hive.external_customer_table_1 (
                     >     customer_id STRING,
                     >     customer_fname STRING,
                     >     customer_lname STRING,
                     >     username STRING,
                     >     password STRING,
                     >     address STRING,
                     >     city STRING,
                     >     state STRING,
                     >     pincode STRING
                     > ) 
                     > CLUSTERED BY (customer_id) into 4 buckets
                     > ROW FORMAT DELIMITED
                     > FIELDS TERMINATED BY ','
                     > STORED AS textfile;
OK
Time taken: 0.841 seconds

hive (misgaurav_hive)> CREATE EXTERNAL TABLE IF NOT EXISTS misgaurav_hive.external_orders_table_1 (
                     >     order_id STRING,
                     >     order_date STRING,
                     >     customer_id STRING,
                     >     order_status STRING
                     > ) 
                     > CLUSTERED BY (customer_id) into 8 buckets
                     > ROW FORMAT DELIMITED
                     > FIELDS TERMINATED BY ','
                     > STORED AS textfile;
OK
Time taken: 0.181 seconds

hive (misgaurav_hive)> show tables;
OK
external_customer_table
external_customer_table_1
external_orders_table
external_orders_table_1
Time taken: 0.265 seconds, Fetched: 4 row(s)

######### load the data into it from the normal table to bucket table ##########
# to disable the map side join #

hive (misgaurav_hive)> set hive.auto.convert.join = false;

hive (misgaurav_hive)> insert into external_customer_table_1 select * from external_customer_table;
Query ID = itv020752_20251218125600_85102348-4dba-417b-86a3-9b4078414488
Total jobs = 2
Launching Job 1 out of 2
Number of reduce tasks determined at compile time: 4
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1766025126719_0348, Tracking URL = http://m02.itversity.com:19088/proxy/application_1766025126719_0348/
Kill Command = /opt/hadoop/bin/mapred job  -kill job_1766025126719_0348
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 4
2025-12-18 12:57:13,327 Stage-1 map = 0%,  reduce = 0%
2025-12-18 12:57:17,445 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 2.16 sec
2025-12-18 12:57:22,573 Stage-1 map = 100%,  reduce = 50%, Cumulative CPU 6.52 sec
2025-12-18 12:57:23,600 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 11.21 sec
MapReduce Total cumulative CPU time: 11 seconds 210 msec
Ended Job = job_1766025126719_0348
Loading data to table misgaurav_hive.external_customer_table_1
Launching Job 2 out of 2
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1766025126719_0349, Tracking URL = http://m02.itversity.com:19088/proxy/application_1766025126719_0349/
Kill Command = /opt/hadoop/bin/mapred job  -kill job_1766025126719_0349
Hadoop job information for Stage-3: number of mappers: 1; number of reducers: 1
2025-12-18 12:57:35,692 Stage-3 map = 0%,  reduce = 0%
2025-12-18 12:57:38,791 Stage-3 map = 100%,  reduce = 0%, Cumulative CPU 1.06 sec
2025-12-18 12:57:42,881 Stage-3 map = 100%,  reduce = 100%, Cumulative CPU 2.65 sec
MapReduce Total cumulative CPU time: 2 seconds 650 msec
Ended Job = job_1766025126719_0349
FAILED: Execution Error, return code 1 from org.apache.hadoop.hive.ql.exec.StatsTask
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 1  Reduce: 4   Cumulative CPU: 11.21 sec   HDFS Read: 1006853 HDFS Write: 972127 SUCCESS
Stage-Stage-3: Map: 1  Reduce: 1   Cumulative CPU: 2.65 sec   HDFS Read: 35960 HDFS Write: 6065 SUCCESS
Total MapReduce CPU Time Spent: 13 seconds 860 msec


hive (misgaurav_hive)> insert into external_orders_table_1 select * from external_orders_table;
Query ID = itv020752_20251218125931_8eef789d-44e6-4e07-b971-28c7eb502112
Total jobs = 2
Launching Job 1 out of 2
Number of reduce tasks determined at compile time: 8
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1766025126719_0354, Tracking URL = http://m02.itversity.com:19088/proxy/application_1766025126719_0354/
Kill Command = /opt/hadoop/bin/mapred job  -kill job_1766025126719_0354
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 8
2025-12-18 13:00:28,505 Stage-1 map = 0%,  reduce = 0%
2025-12-18 13:00:33,710 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 3.57 sec
2025-12-18 13:00:39,855 Stage-1 map = 100%,  reduce = 63%, Cumulative CPU 17.68 sec
2025-12-18 13:00:40,909 Stage-1 map = 100%,  reduce = 75%, Cumulative CPU 20.27 sec
2025-12-18 13:00:41,928 Stage-1 map = 100%,  reduce = 88%, Cumulative CPU 23.41 sec
2025-12-18 13:00:42,947 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 26.45 sec
MapReduce Total cumulative CPU time: 26 seconds 450 msec
Ended Job = job_1766025126719_0354
Loading data to table misgaurav_hive.external_orders_table_1
Launching Job 2 out of 2
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1766025126719_0356, Tracking URL = http://m02.itversity.com:19088/proxy/application_1766025126719_0356/
Kill Command = /opt/hadoop/bin/mapred job  -kill job_1766025126719_0356
Hadoop job information for Stage-3: number of mappers: 1; number of reducers: 1
2025-12-18 13:00:56,368 Stage-3 map = 0%,  reduce = 0%
2025-12-18 13:01:00,528 Stage-3 map = 100%,  reduce = 0%, Cumulative CPU 1.63 sec
2025-12-18 13:01:05,610 Stage-3 map = 100%,  reduce = 100%, Cumulative CPU 3.88 sec
MapReduce Total cumulative CPU time: 3 seconds 880 msec
Ended Job = job_1766025126719_0356
FAILED: Execution Error, return code 1 from org.apache.hadoop.hive.ql.exec.StatsTask
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 1  Reduce: 8   Cumulative CPU: 26.45 sec   HDFS Read: 3073625 HDFS Write: 3017560 SUCCESS
Stage-Stage-3: Map: 1  Reduce: 1   Cumulative CPU: 3.88 sec   HDFS Read: 31019 HDFS Write: 2996 SUCCESS
Total MapReduce CPU Time Spent: 30 seconds 330 msec


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
#  Database(name='misgaurav_hive_new', description='', locationUri='hdfs://m01.itversity.com:9000/user/itv020752/warehouse/misgaurav_hive_new.db'),
#  Database(name='misgaurav_hive_part1', description='', locationUri='hdfs://m01.itversity.com:9000/user/itv020752/warehouse/misgaurav_hive_part1.db')] 
# [Table(name='external_customer_table', database='misgaurav_hive', description=None, tableType='EXTERNAL', isTemporary=False),
#  Table(name='external_customer_table_1', database='misgaurav_hive', description=None, tableType='EXTERNAL', isTemporary=False),
#  Table(name='external_orders_table', database='misgaurav_hive', description=None, tableType='EXTERNAL', isTemporary=False),
#  Table(name='external_orders_table_1', database='misgaurav_hive', description=None, tableType='EXTERNAL', isTemporary=False)]

!hadoop fs -ls /user/itv020752/warehouse/misgaurav_hive.db/
# Found 2 items
# drwxr-xr-x   - itv020752 supergroup          0 2025-12-18 06:23 /user/itv020752/warehouse/misgaurav_hive.db/external_customer_table_1
# drwxr-xr-x   - itv020752 supergroup          0 2025-12-18 06:29 /user/itv020752/warehouse/misgaurav_hive.db/external_orders_table_1

!hadoop fs -ls -h /user/itv020752/warehouse/misgaurav_hive.db/external_customer_table_1 
# Found 4 items
# -rw-r--r--   3 itv020752 supergroup    232.5 K 2025-12-18 12:57 /user/itv020752/warehouse/misgaurav_hive.db/external_customer_table_1/000000_0
# -rw-r--r--   3 itv020752 supergroup    233.5 K 2025-12-18 12:57 /user/itv020752/warehouse/misgaurav_hive.db/external_customer_table_1/000001_0
# -rw-r--r--   3 itv020752 supergroup    232.4 K 2025-12-18 12:57 /user/itv020752/warehouse/misgaurav_hive.db/external_customer_table_1/000002_0
# -rw-r--r--   3 itv020752 supergroup    232.9 K 2025-12-18 12:57 /user/itv020752/warehouse/misgaurav_hive.db/external_customer_table_1/000003_0

!hadoop fs -ls -h /user/itv020752/warehouse/misgaurav_hive.db/external_orders_table_1
# Found 8 items
# -rw-r--r--   3 itv020752 supergroup    365.2 K 2025-12-18 13:00 /user/itv020752/warehouse/misgaurav_hive.db/external_orders_table_1/000000_0
# -rw-r--r--   3 itv020752 supergroup    363.6 K 2025-12-18 13:00 /user/itv020752/warehouse/misgaurav_hive.db/external_orders_table_1/000001_0
# -rw-r--r--   3 itv020752 supergroup    367.9 K 2025-12-18 13:00 /user/itv020752/warehouse/misgaurav_hive.db/external_orders_table_1/000002_0
# -rw-r--r--   3 itv020752 supergroup    370.5 K 2025-12-18 13:00 /user/itv020752/warehouse/misgaurav_hive.db/external_orders_table_1/000003_0
# -rw-r--r--   3 itv020752 supergroup    365.2 K 2025-12-18 13:00 /user/itv020752/warehouse/misgaurav_hive.db/external_orders_table_1/000004_0
# -rw-r--r--   3 itv020752 supergroup    365.8 K 2025-12-18 13:00 /user/itv020752/warehouse/misgaurav_hive.db/external_orders_table_1/000005_0
# -rw-r--r--   3 itv020752 supergroup    362.7 K 2025-12-18 13:00 /user/itv020752/warehouse/misgaurav_hive.db/external_orders_table_1/000006_0
# -rw-r--r--   3 itv020752 supergroup    368.7 K 2025-12-18 13:00 /user/itv020752/warehouse/misgaurav_hive.db/external_orders_table_1/000007_0

hive (misgaurav_hive)> set hive.enforce.bucketing=true;
hive (misgaurav_hive)> set hive.optimize.bucketmapjoin = true;

hive (misgaurav_hive)> select O.*, C.* from external_orders_table_1 O inner join external_customer_table_1 C
                     > on O.customer_id = C.customer_id limit 5;
Query ID = itv020752_20251218130616_1360e44d-a55e-4724-a1bc-bb7c75912570
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1766025126719_0360, Tracking URL = http://m02.itversity.com:19088/proxy/application_1766025126719_0360/
Kill Command = /opt/hadoop/bin/mapred job  -kill job_1766025126719_0360
Hadoop job information for Stage-1: number of mappers: 2; number of reducers: 1
2025-12-18 13:07:05,568 Stage-1 map = 0%,  reduce = 0%
2025-12-18 13:07:10,682 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 7.49 sec
2025-12-18 13:07:15,823 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 9.59 sec
MapReduce Total cumulative CPU time: 9 seconds 590 msec
Ended Job = job_1766025126719_0360
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 2  Reduce: 1   Cumulative CPU: 9.59 sec   HDFS Read: 3980320 HDFS Write: 740 SUCCESS
Total MapReduce CPU Time Spent: 9 seconds 590 msec
OK
22945   2013-12-13 00:00:00.0   1       COMPLETE        1       Richard Hernandez       XXXXXXXXX       XXXXXXXXX       6303 Heather PlazaB
rownsville      TX      78521
56133   2014-07-15 00:00:00.0   10      COMPLETE        10      Melissa Smith   XXXXXXXXX       XXXXXXXXX       8598 Harvest Beacon Plaza S
tafford VA      22554
45239   2014-05-01 00:00:00.0   10      COMPLETE        10      Melissa Smith   XXXXXXXXX       XXXXXXXXX       8598 Harvest Beacon Plaza S
tafford VA      22554
15045   2013-10-28 00:00:00.0   100     PROCESSING      100     George  Barrett XXXXXXXXX       XXXXXXXXX       4110 Silent Pointe      Cag
uas     PR      00725
28477   2014-01-16 00:00:00.0   100     COMPLETE        100     George  Barrett XXXXXXXXX       XXXXXXXXX       4110 Silent Pointe      Cag
uas     PR      00725
Time taken: 61.426 seconds, Fetched: 5 row(s)

hive (misgaurav_hive)> explain extended select O.*, C.* from external_orders_table_1 O inner join external_customer_table_1 C
                     > on O.customer_id = C.customer_id limit 5;
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
            Statistics: Num rows: 68883 Data size: 2931061 Basic stats: COMPLETE Column stats: NONE
            GatherStats: false
            Filter Operator
              isSamplingPred: false
              predicate: customer_id is not null (type: boolean)
              Statistics: Num rows: 68883 Data size: 2931061 Basic stats: COMPLETE Column stats: NONE
              Select Operator
                expressions: order_id (type: string), order_date (type: string), customer_id (type: string), order_status (type: string)
                outputColumnNames: _col0, _col1, _col2, _col3
                Statistics: Num rows: 68883 Data size: 2931061 Basic stats: COMPLETE Column stats: NONE
                Reduce Output Operator
                  key expressions: _col2 (type: string)
                  null sort order: a
                  sort order: +
                  Map-reduce partition columns: _col2 (type: string)
                  Statistics: Num rows: 68883 Data size: 2931061 Basic stats: COMPLETE Column stats: NONE
                  tag: 0
                  value expressions: _col0 (type: string), _col1 (type: string), _col3 (type: string)
                  auto parallelism: false
          TableScan
            alias: c
            Statistics: Num rows: 24870 Data size: 1882568 Basic stats: COMPLETE Column stats: NONE
            GatherStats: false
            Filter Operator
              isSamplingPred: false
              predicate: customer_id is not null (type: boolean)
              Statistics: Num rows: 24870 Data size: 1882568 Basic stats: COMPLETE Column stats: NONE
              Select Operator
                expressions: customer_id (type: string), customer_fname (type: string), customer_lname (type: string), username (type: string), password (type: string), address (type: string), city (type: string), state (type: string), pincode (type: string)
                outputColumnNames: _col0, _col1, _col2, _col3, _col4, _col5, _col6, _col7, _col8
                Statistics: Num rows: 24870 Data size: 1882568 Basic stats: COMPLETE Column stats: NONE
                Reduce Output Operator
                  key expressions: _col0 (type: string)
                  null sort order: a
                  sort order: +
                  Map-reduce partition columns: _col0 (type: string)
                  Statistics: Num rows: 24870 Data size: 1882568 Basic stats: COMPLETE Column stats: NONE
                  tag: 1
                  value expressions: _col1 (type: string), _col2 (type: string), _col3 (type: string), _col4 (type: string), _col5 (type: string), _col6 (type: string), _col7 (type: string), _col8 (type: string)
                  auto parallelism: false
      Path -> Alias:
        hdfs://m01.itversity.com:9000/user/itv020752/warehouse/misgaurav_hive.db/external_customer_table_1 [$hdt$_1:c]
        hdfs://m01.itversity.com:9000/user/itv020752/warehouse/misgaurav_hive.db/external_orders_table_1 [$hdt$_0:o]
      Path -> Partition:
        hdfs://m01.itversity.com:9000/user/itv020752/warehouse/misgaurav_hive.db/external_customer_table_1 
          Partition
            base file name: external_customer_table_1
            input format: org.apache.hadoop.mapred.TextInputFormat
            output format: org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
            properties:
              COLUMN_STATS_ACCURATE {"BASIC_STATS":"true","COLUMN_STATS":{"address":"true","city":"true","customer_fname":"true","customer_id":"true","customer_lname":"true","password":"true","pincode":"true","state":"true","username":"true"}}
              EXTERNAL TRUE
              bucket_count 4
              bucket_field_name customer_id
              bucketing_version 2
              column.name.delimiter ,
              columns customer_id,customer_fname,customer_lname,username,password,address,city,state,pincode
              columns.comments 
              columns.types string:string:string:string:string:string:string:string:string
              field.delim ,
              file.inputformat org.apache.hadoop.mapred.TextInputFormat
              file.outputformat org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
              location hdfs://m01.itversity.com:9000/user/itv020752/warehouse/misgaurav_hive.db/external_customer_table_1
              name misgaurav_hive.external_customer_table_1
              numFiles 4
              numRows 24870
              rawDataSize 1882568
              serialization.ddl struct external_customer_table_1 { string customer_id, string customer_fname, string customer_lname, string username, string password, string address, string city, string state, string pincode}
              serialization.format ,
              serialization.lib org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
              totalSize 953719
              transient_lastDdlTime 1766080665
            serde: org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
          
              input format: org.apache.hadoop.mapred.TextInputFormat
              output format: org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
              properties:
                COLUMN_STATS_ACCURATE {"BASIC_STATS":"true","COLUMN_STATS":{"address":"true","city":"true","customer_fname":"true","customer_id":"true","customer_lname":"true","password":"true","pincode":"true","state":"true","username":"true"}}
                EXTERNAL TRUE
                bucket_count 4
                bucket_field_name customer_id
                bucketing_version 2
                column.name.delimiter ,
                columns customer_id,customer_fname,customer_lname,username,password,address,city,state,pincode
                columns.comments 
                columns.types string:string:string:string:string:string:string:string:string
                field.delim ,
                file.inputformat org.apache.hadoop.mapred.TextInputFormat
                file.outputformat org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
                location hdfs://m01.itversity.com:9000/user/itv020752/warehouse/misgaurav_hive.db/external_customer_table_1
                name misgaurav_hive.external_customer_table_1
                numFiles 4
                numRows 24870
                rawDataSize 1882568
                serialization.ddl struct external_customer_table_1 { string customer_id, string customer_fname, string customer_lname, string username, string password, string address, string city, string state, string pincode}
                serialization.format ,
                serialization.lib org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
                totalSize 953719
                transient_lastDdlTime 1766080665
              serde: org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
              name: misgaurav_hive.external_customer_table_1
            name: misgaurav_hive.external_customer_table_1
        hdfs://m01.itversity.com:9000/user/itv020752/warehouse/misgaurav_hive.db/external_orders_table_1 
          Partition
            base file name: external_orders_table_1
            input format: org.apache.hadoop.mapred.TextInputFormat
            output format: org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
            properties:
              COLUMN_STATS_ACCURATE {"BASIC_STATS":"true","COLUMN_STATS":{"customer_id":"true","order_date":"true","order_id":"true","order_status":"true"}}
              EXTERNAL TRUE
              bucket_count 8
              bucket_field_name customer_id
              bucketing_version 2
              column.name.delimiter ,
              columns order_id,order_date,customer_id,order_status
              columns.comments 
              columns.types string:string:string:string
              field.delim ,
              file.inputformat org.apache.hadoop.mapred.TextInputFormat
              file.outputformat org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
              location hdfs://m01.itversity.com:9000/user/itv020752/warehouse/misgaurav_hive.db/external_orders_table_1
              name misgaurav_hive.external_orders_table_1
              numFiles 8
              numRows 68883
              rawDataSize 2931061
              serialization.ddl struct external_orders_table_1 { string order_id, string order_date, string customer_id, string order_status}
              serialization.format ,
              serialization.lib org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
              totalSize 2999944
              transient_lastDdlTime 1766080867
            serde: org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
          
              input format: org.apache.hadoop.mapred.TextInputFormat
              output format: org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
              properties:
                COLUMN_STATS_ACCURATE {"BASIC_STATS":"true","COLUMN_STATS":{"customer_id":"true","order_date":"true","order_id":"true","order_status":"true"}}
                EXTERNAL TRUE
                bucket_count 8
                bucket_field_name customer_id
                bucketing_version 2
                column.name.delimiter ,
                columns order_id,order_date,customer_id,order_status
                columns.comments 
                columns.types string:string:string:string
                field.delim ,
                file.inputformat org.apache.hadoop.mapred.TextInputFormat
                file.outputformat org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
                location hdfs://m01.itversity.com:9000/user/itv020752/warehouse/misgaurav_hive.db/external_orders_table_1
                name misgaurav_hive.external_orders_table_1
                numFiles 8
                numRows 68883
                rawDataSize 2931061
                serialization.ddl struct external_orders_table_1 { string order_id, string order_date, string customer_id, string order_status}
                serialization.format ,
                serialization.lib org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
                totalSize 2999944
                transient_lastDdlTime 1766080867
              serde: org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
              name: misgaurav_hive.external_orders_table_1
            name: misgaurav_hive.external_orders_table_1
      Truncated Path -> Alias:
        hdfs://m01.itversity.com:9000/user/itv020752/warehouse/misgaurav_hive.db/external_customer_table_1 [$hdt$_1:c]
        hdfs://m01.itversity.com:9000/user/itv020752/warehouse/misgaurav_hive.db/external_orders_table_1 [$hdt$_0:o]
      Needs Tagging: true
      Reduce Operator Tree:
        Join Operator
          condition map:
               Inner Join 0 to 1
          keys:
            0 _col2 (type: string)
            1 _col0 (type: string)
          outputColumnNames: _col0, _col1, _col2, _col3, _col4, _col5, _col6, _col7, _col8, _col9, _col10, _col11, _col12
          Statistics: Num rows: 75771 Data size: 3224167 Basic stats: COMPLETE Column stats: NONE
          Limit
            Number of rows: 5
            Statistics: Num rows: 5 Data size: 210 Basic stats: COMPLETE Column stats: NONE
            File Output Operator
              compressed: false
              GlobalTableId: 0
              directory: hdfs://m01.itversity.com:9000/tmp/hive/itv020752/itv020752/72fa5fea-f7de-48fc-921f-e3ac6a8771a3/hive_2025-12-18_13-08-00_078_1460106032218598282-2/-mr-10001/.hive-staging_hive_2025-12-18_13-08-00_078_1460106032218598282-2/-ext-10002
              NumFilesPerFileSink: 1
              Statistics: Num rows: 5 Data size: 210 Basic stats: COMPLETE Column stats: NONE
              Stats Publishing Key Prefix: hdfs://m01.itversity.com:9000/tmp/hive/itv020752/itv020752/72fa5fea-f7de-48fc-921f-e3ac6a8771a3/hive_2025-12-18_13-08-00_078_1460106032218598282-2/-mr-10001/.hive-staging_hive_2025-12-18_13-08-00_078_1460106032218598282-2/-ext-10002/
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

Time taken: 47.558 seconds, Fetched: 215 row(s)



###########################################################################################################################

now do this 

hive (misgaurav_hive)> set hive.auto.convert.join = true;
hive (misgaurav_hive)> set hive.enforce.bucketing=true;
hive (misgaurav_hive)> set hive.optimize.bucketmapjoin = true;

hive (misgaurav_hive)> select O.*, C.* from external_orders_table_1 O inner join external_customer_table_1 C
                     > on O.customer_id = C.customer_id limit 5;
Query ID = itv020752_20251218131100_07aefc3d-7094-4203-923c-0a3f5998bad4
Total jobs = 1
SLF4J: Found binding in [jar:file:/opt/apache-hive-3.1.2-bin/lib/log4j-slf4j-impl-2.10.0.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/opt/hadoop-3.3.0/share/hadoop/common/lib/slf4j-log4j12-1.7.25.jar!/org/slf4j/impl/StaticLoggerBinder.cla
ss]

2025-12-18 13:11:48     Starting to launch local task to process map join;      maximum memory = 14316732416
2025-12-18 13:11:49     Dump the side-table for tag: 1 with group count: 12435 into file: file:/tmp/itv020752/72fa5fea-f7de-48fc-921f-e3ac6
a8771a3/hive_2025-12-18_13-11-00_422_4949971555805847195-2/-local-10004/HashTable-Stage-3/MapJoin-mapfile31--.hashtable2025-12-18 13:11:49U
ploaded 1 File to: file:/tmp/itv020752/72fa5fea-f7de-48fc-921f-e3ac6a8771a3/hive_2025-12-18_13-11-00_422_4949971555805847195-2/-local-10004
/HashTable-Stage-3/MapJoin-mapfile31--.hashtable (1183557 bytes)
Execution completed successfully
MapredLocal task succeeded
Launching Job 1 out of 1
Number of reduce tasks is set to 0 since there's no reduce operator
Starting Job = job_1766025126719_0364, Tracking URL = http://m02.itversity.com:19088/proxy/application_1766025126719_0364/
Kill Command = /opt/hadoop/bin/mapred job  -kill job_1766025126719_0364
Hadoop job information for Stage-3: number of mappers: 1; number of reducers: 0
2025-12-18 13:11:59,228 Stage-3 map = 0%,  reduce = 0%
2025-12-18 13:12:04,330 Stage-3 map = 100%,  reduce = 0%, Cumulative CPU 2.45 sec
MapReduce Total cumulative CPU time: 2 seconds 450 msec
Ended Job = job_1766025126719_0364
MapReduce Jobs Launched: 
Stage-Stage-3: Map: 1   Cumulative CPU: 2.45 sec   HDFS Read: 282946 HDFS Write: 740 SUCCESS
Total MapReduce CPU Time Spent: 2 seconds 450 msec
OK
30725   2014-01-31 00:00:00.0   9779    PENDING_PAYMENT 9779    Mary    Smith   XXXXXXXXX       XXXXXXXXX       1102 Cozy Wynd  Baltimore M
D       21218
51497   2014-06-13 00:00:00.0   3047    PENDING 3047    Barbara Smith   XXXXXXXXX       XXXXXXXXX       8549 Dusty View Mountain        Cag
uas     PR      00725
60292   2013-11-02 00:00:00.0   5841    COMPLETE        5841    Mary    Smith   XXXXXXXXX       XXXXXXXXX       3076 Indian Mews        Cag
uas     PR      00725
60291   2013-11-02 00:00:00.0   682     COMPLETE        682     Emma    Smith   XXXXXXXXX       XXXXXXXXX       1440 Burning Bluff Park Salinas    CA      93906
66713   2014-06-29 00:00:00.0   11602   COMPLETE        11602   Mary    Smith   XXXXXXXXX       XXXXXXXXX       6292 Easy Zephyr Pike   Mechanicsburg      PA      17055
Time taken: 66.053 seconds, Fetched: 5 row(s)

hive (misgaurav_hive)> explain extended select O.*, C.* from external_orders_table_1 O inner join external_customer_table_1 C
                     > on O.customer_id = C.customer_id limit 5;
OK
STAGE DEPENDENCIES:
  Stage-4 is a root stage
  Stage-3 depends on stages: Stage-4
  Stage-0 depends on stages: Stage-3

STAGE PLANS:
  Stage: Stage-4
    Map Reduce Local Work
      Alias -> Map Local Tables:
        $hdt$_1:c 
          Fetch Operator
            limit: -1
      Alias -> Map Local Operator Tree:
        $hdt$_1:c 
          TableScan
            alias: c
            Statistics: Num rows: 24870 Data size: 1882568 Basic stats: COMPLETE Column stats: NONE
            GatherStats: false
            Filter Operator
              isSamplingPred: false
              predicate: customer_id is not null (type: boolean)
              Statistics: Num rows: 24870 Data size: 1882568 Basic stats: COMPLETE Column stats: NONE
              Select Operator
                expressions: customer_id (type: string), customer_fname (type: string), customer_lname (type: string), username (type: stri
ng), password (type: string), address (type: string), city (type: string), state (type: string), pincode (type: string)
                outputColumnNames: _col0, _col1, _col2, _col3, _col4, _col5, _col6, _col7, _col8
                Statistics: Num rows: 24870 Data size: 1882568 Basic stats: COMPLETE Column stats: NONE
                HashTable Sink Operator
                  keys:
                    0 _col2 (type: string)
                    1 _col0 (type: string)
                  Position of Big Table: 0

  Stage: Stage-3
    Map Reduce
      Map Operator Tree:
          TableScan
            alias: o
            Statistics: Num rows: 68883 Data size: 2931061 Basic stats: COMPLETE Column stats: NONE
            GatherStats: false
            Filter Operator
              isSamplingPred: false
              predicate: customer_id is not null (type: boolean)
              Statistics: Num rows: 68883 Data size: 2931061 Basic stats: COMPLETE Column stats: NONE
              Select Operator
                expressions: order_id (type: string), order_date (type: string), customer_id (type: string), order_status (type: string)
                outputColumnNames: _col0, _col1, _col2, _col3
                Statistics: Num rows: 68883 Data size: 2931061 Basic stats: COMPLETE Column stats: NONE
                Map Join Operator
                  condition map:
                       Inner Join 0 to 1
                  keys:
                    0 _col2 (type: string)
                    1 _col0 (type: string)
                  outputColumnNames: _col0, _col1, _col2, _col3, _col4, _col5, _col6, _col7, _col8, _col9, _col10, _col11, _col12
                  Position of Big Table: 0
                  Statistics: Num rows: 75771 Data size: 3224167 Basic stats: COMPLETE Column stats: NONE
                  Limit
                    Number of rows: 5
                    Statistics: Num rows: 5 Data size: 210 Basic stats: COMPLETE Column stats: NONE
                    File Output Operator
                      compressed: false
                      GlobalTableId: 0
                      directory: hdfs://m01.itversity.com:9000/tmp/hive/itv020752/itv020752/72fa5fea-f7de-48fc-921f-e3ac6a8771a3/hive_2025-12-18_13-12-36_045_5317643422509042844-2/-mr-10001/.hive-staging_hive_2025-12-18_13-12-36_045_5317643422509042844-2/-ext-10002
                      NumFilesPerFileSink: 1
                      Statistics: Num rows: 5 Data size: 210 Basic stats: COMPLETE Column stats: NONE
                      Stats Publishing Key Prefix: hdfs://m01.itversity.com:9000/tmp/hive/itv020752/itv020752/72fa5fea-f7de-48fc-921f-e3ac6a8771a3/hive_2025-12-18_13-12-36_045_5317643422509042844-2/-mr-10001/.hive-staging_hive_2025-12-18_13-12-36_045_5317643422509042844-2/-ext-10002/
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
      Execution mode: vectorized
      Local Work:
        Map Reduce Local Work
      Path -> Alias:
        hdfs://m01.itversity.com:9000/user/itv020752/warehouse/misgaurav_hive.db/external_orders_table_1 [$hdt$_0:o]
      Path -> Partition:
        hdfs://m01.itversity.com:9000/user/itv020752/warehouse/misgaurav_hive.db/external_customer_table_1 
          Partition
            base file name: external_customer_table_1
            input format: org.apache.hadoop.mapred.TextInputFormat
            output format: org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
            properties:
              COLUMN_STATS_ACCURATE {"BASIC_STATS":"true","COLUMN_STATS":{"address":"true","city":"true","customer_fname":"true","customer_id":"true","customer_lname":"true","password":"true","pincode":"true","state":"true","username":"true"}}
              EXTERNAL TRUE
              bucket_count 4
              bucket_field_name customer_id
              bucketing_version 2
              column.name.delimiter ,
              columns customer_id,customer_fname,customer_lname,username,password,address,city,state,pincode
              columns.comments 
              columns.types string:string:string:string:string:string:string:string:string
              field.delim ,
              file.inputformat org.apache.hadoop.mapred.TextInputFormat
              file.outputformat org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
              location hdfs://m01.itversity.com:9000/user/itv020752/warehouse/misgaurav_hive.db/external_customer_table_1
              name misgaurav_hive.external_customer_table_1
              numFiles 4
              numRows 24870
              rawDataSize 1882568
              serialization.ddl struct external_customer_table_1 { string customer_id, string customer_fname, string customer_lname, string username, string password, string address, string city, string state, string pincode}
              serialization.format ,
              serialization.lib org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
              totalSize 953719
              transient_lastDdlTime 1766080665
            serde: org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
          
              input format: org.apache.hadoop.mapred.TextInputFormat
              output format: org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
              properties:
                COLUMN_STATS_ACCURATE {"BASIC_STATS":"true","COLUMN_STATS":{"address":"true","city":"true","customer_fname":"true","customer_id":"true","customer_lname":"true","password":"true","pincode":"true","state":"true","username":"true"}}
                EXTERNAL TRUE
                bucket_count 4
                bucket_field_name customer_id
                bucketing_version 2
                column.name.delimiter ,
                columns customer_id,customer_fname,customer_lname,username,password,address,city,state,pincode
                columns.comments 
                columns.types string:string:string:string:string:string:string:string:string
                field.delim ,
                file.inputformat org.apache.hadoop.mapred.TextInputFormat
                file.outputformat org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
                location hdfs://m01.itversity.com:9000/user/itv020752/warehouse/misgaurav_hive.db/external_customer_table_1
                name misgaurav_hive.external_customer_table_1
                numFiles 4
                numRows 24870
                rawDataSize 1882568
                serialization.ddl struct external_customer_table_1 { string customer_id, string customer_fname, string customer_lname, string username, string password, string address, string city, string state, string pincode}
                serialization.format ,
                serialization.lib org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
                totalSize 953719
                transient_lastDdlTime 1766080665
              serde: org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
              name: misgaurav_hive.external_customer_table_1
            name: misgaurav_hive.external_customer_table_1
        hdfs://m01.itversity.com:9000/user/itv020752/warehouse/misgaurav_hive.db/external_orders_table_1 
          Partition
            base file name: external_orders_table_1
            input format: org.apache.hadoop.mapred.TextInputFormat
            output format: org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
            properties:
              COLUMN_STATS_ACCURATE {"BASIC_STATS":"true","COLUMN_STATS":{"customer_id":"true","order_date":"true","order_id":"true","order_status":"true"}}
              EXTERNAL TRUE
              bucket_count 8
              bucket_field_name customer_id
              bucketing_version 2
              column.name.delimiter ,
              columns order_id,order_date,customer_id,order_status
              columns.comments 
              columns.types string:string:string:string
              field.delim ,
              file.inputformat org.apache.hadoop.mapred.TextInputFormat
              file.outputformat org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
              location hdfs://m01.itversity.com:9000/user/itv020752/warehouse/misgaurav_hive.db/external_orders_table_1
              name misgaurav_hive.external_orders_table_1
              numFiles 8
              numRows 68883
              rawDataSize 2931061
              serialization.ddl struct external_orders_table_1 { string order_id, string order_date, string customer_id, string order_status}
              serialization.format ,
              serialization.lib org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
              totalSize 2999944
              transient_lastDdlTime 1766080867
            serde: org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
          
              input format: org.apache.hadoop.mapred.TextInputFormat
              output format: org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
              properties:
                COLUMN_STATS_ACCURATE {"BASIC_STATS":"true","COLUMN_STATS":{"customer_id":"true","order_date":"true","order_id":"true","order_status":"true"}}
                EXTERNAL TRUE
                bucket_count 8
                bucket_field_name customer_id
                bucketing_version 2
                column.name.delimiter ,
                columns order_id,order_date,customer_id,order_status
                columns.comments 
                columns.types string:string:string:string
                field.delim ,
                file.inputformat org.apache.hadoop.mapred.TextInputFormat
                file.outputformat org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
                location hdfs://m01.itversity.com:9000/user/itv020752/warehouse/misgaurav_hive.db/external_orders_table_1
                name misgaurav_hive.external_orders_table_1
                numFiles 8
                numRows 68883
                rawDataSize 2931061
                serialization.ddl struct external_orders_table_1 { string order_id, string order_date, string customer_id, string order_status}
                serialization.format ,
                serialization.lib org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
                totalSize 2999944
                transient_lastDdlTime 1766080867
              serde: org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
              name: misgaurav_hive.external_orders_table_1
            name: misgaurav_hive.external_orders_table_1
      Truncated Path -> Alias:
        hdfs://m01.itversity.com:9000/user/itv020752/warehouse/misgaurav_hive.db/external_orders_table_1 [$hdt$_0:o]

  Stage: Stage-0
    Fetch Operator
      limit: 5
      Processor Tree:
        ListSink

Time taken: 60.153 seconds, Fetched: 212 row(s)
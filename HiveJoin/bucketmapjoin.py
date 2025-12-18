# here the table is very small, not big enough as expected for a bucket map join. 
# thus the join falls back to a common join. see line no : 377, & 508 in this file.

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
Query ID = itv020752_20251218062227_4d7561ff-99fa-4f30-a6d1-8f0b967c0235
Total jobs = 2
Launching Job 1 out of 2
Number of reduce tasks determined at compile time: 4
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1766025126719_0196, Tracking URL = http://m02.itversity.com:19088/proxy/application_1766025126719_0196/
Kill Command = /opt/hadoop/bin/mapred job  -kill job_1766025126719_0196
Hadoop job information for Stage-1: number of mappers: 0; number of reducers: 4
2025-12-18 06:23:25,686 Stage-1 map = 0%,  reduce = 0%
2025-12-18 06:23:30,880 Stage-1 map = 0%,  reduce = 25%, Cumulative CPU 1.63 sec
2025-12-18 06:23:31,909 Stage-1 map = 0%,  reduce = 100%, Cumulative CPU 7.22 sec
MapReduce Total cumulative CPU time: 7 seconds 220 msec
Ended Job = job_1766025126719_0196
Loading data to table misgaurav_hive.external_customer_table_1
Launching Job 2 out of 2
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1766025126719_0198, Tracking URL = http://m02.itversity.com:19088/proxy/application_1766025126719_0198/
Kill Command = /opt/hadoop/bin/mapred job  -kill job_1766025126719_0198
Hadoop job information for Stage-3: number of mappers: 1; number of reducers: 1
2025-12-18 06:23:44,219 Stage-3 map = 0%,  reduce = 0%
2025-12-18 06:23:48,425 Stage-3 map = 100%,  reduce = 0%, Cumulative CPU 1.16 sec
2025-12-18 06:23:53,549 Stage-3 map = 100%,  reduce = 100%, Cumulative CPU 2.81 sec
MapReduce Total cumulative CPU time: 2 seconds 810 msec
Ended Job = job_1766025126719_0198
MapReduce Jobs Launched: 
Stage-Stage-1: Reduce: 4   Cumulative CPU: 7.22 sec   HDFS Read: 46448 HDFS Write: 1332 SUCCESS
Stage-Stage-3: Map: 1  Reduce: 1   Cumulative CPU: 2.81 sec   HDFS Read: 19022 HDFS Write: 262 SUCCESS
Total MapReduce CPU Time Spent: 10 seconds 30 msec
OK
Time taken: 89.123 seconds


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
# -rw-r--r--   3 itv020752 supergroup          0 2025-12-18 06:23 /user/itv020752/warehouse/misgaurav_hive.db/external_customer_table_1/000000_0
# -rw-r--r--   3 itv020752 supergroup          0 2025-12-18 06:23 /user/itv020752/warehouse/misgaurav_hive.db/external_customer_table_1/000001_0
# -rw-r--r--   3 itv020752 supergroup          0 2025-12-18 06:23 /user/itv020752/warehouse/misgaurav_hive.db/external_customer_table_1/000002_0
# -rw-r--r--   3 itv020752 supergroup          0 2025-12-18 06:23 /user/itv020752/warehouse/misgaurav_hive.db/external_customer_table_1/000003_0


!hadoop fs -ls -h /user/itv020752/warehouse/misgaurav_hive.db/external_orders_table_1
# Found 8 items
# -rw-r--r--   3 itv020752 supergroup          0 2025-12-18 06:29 /user/itv020752/warehouse/misgaurav_hive.db/external_orders_table_1/000000_0
# -rw-r--r--   3 itv020752 supergroup          0 2025-12-18 06:29 /user/itv020752/warehouse/misgaurav_hive.db/external_orders_table_1/000001_0
# -rw-r--r--   3 itv020752 supergroup          0 2025-12-18 06:29 /user/itv020752/warehouse/misgaurav_hive.db/external_orders_table_1/000002_0
# -rw-r--r--   3 itv020752 supergroup          0 2025-12-18 06:29 /user/itv020752/warehouse/misgaurav_hive.db/external_orders_table_1/000003_0
# -rw-r--r--   3 itv020752 supergroup          0 2025-12-18 06:29 /user/itv020752/warehouse/misgaurav_hive.db/external_orders_table_1/000004_0
# -rw-r--r--   3 itv020752 supergroup          0 2025-12-18 06:29 /user/itv020752/warehouse/misgaurav_hive.db/external_orders_table_1/000005_0
# -rw-r--r--   3 itv020752 supergroup          0 2025-12-18 06:29 /user/itv020752/warehouse/misgaurav_hive.db/external_orders_table_1/000006_0
# -rw-r--r--   3 itv020752 supergroup          0 2025-12-18 06:29 /user/itv020752/warehouse/misgaurav_hive.db/external_orders_table_1/000007_0

hive (misgaurav_hive)> set hive.enforce.bucketing=true;
hive (misgaurav_hive)> set hive.optimize.bucketmapjoin = true;

hive (misgaurav_hive)> select O.*, C.* from external_orders_table_1 O inner join external_customer_table_1 C
                     > on O.customer_id = C.customer_id limit 5;
Query ID = itv020752_20251218063932_c73c0b2f-fc1b-43a5-a935-95bb501aac53
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1766025126719_0206, Tracking URL = http://m02.itversity.com:19088/proxy/application_1766025126719_0206/
Kill Command = /opt/hadoop/bin/mapred job  -kill job_1766025126719_0206
Hadoop job information for Stage-1: number of mappers: 0; number of reducers: 1
2025-12-18 06:40:57,022 Stage-1 map = 0%,  reduce = 0%
2025-12-18 06:41:02,235 Stage-1 map = 0%,  reduce = 100%, Cumulative CPU 1.31 sec
MapReduce Total cumulative CPU time: 1 seconds 310 msec
Ended Job = job_1766025126719_0206
MapReduce Jobs Launched: 
Stage-Stage-1: Reduce: 1   Cumulative CPU: 1.31 sec   HDFS Read: 8241 HDFS Write: 87 SUCCESS
Total MapReduce CPU Time Spent: 1 seconds 310 msec
OK
Time taken: 92.085 seconds

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
            Statistics: Num rows: 1 Data size: 0 Basic stats: PARTIAL Column stats: NONE
            GatherStats: false
            Filter Operator
              isSamplingPred: false
              predicate: customer_id is not null (type: boolean)
              Statistics: Num rows: 1 Data size: 0 Basic stats: PARTIAL Column stats: NONE
              Select Operator
                expressions: order_id (type: string), order_date (type: string), customer_id (type: string), order_status (type: string)
                outputColumnNames: _col0, _col1, _col2, _col3
                Statistics: Num rows: 1 Data size: 0 Basic stats: PARTIAL Column stats: NONE
                Reduce Output Operator
                  key expressions: _col2 (type: string)
                  null sort order: a
                  sort order: +
                  Map-reduce partition columns: _col2 (type: string)
                  Statistics: Num rows: 1 Data size: 0 Basic stats: PARTIAL Column stats: NONE
                  tag: 0
                  value expressions: _col0 (type: string), _col1 (type: string), _col3 (type: string)
                  auto parallelism: false
          TableScan
            alias: c
            Statistics: Num rows: 1 Data size: 0 Basic stats: PARTIAL Column stats: NONE
            GatherStats: false
            Filter Operator
              isSamplingPred: false
              predicate: customer_id is not null (type: boolean)
              Statistics: Num rows: 1 Data size: 0 Basic stats: PARTIAL Column stats: NONE
              Select Operator
                expressions: customer_id (type: string), customer_fname (type: string), customer_lname (type: string), username (type: string), password (type: string), address (type: string), city (type: string), state (type: string), pincode (type: string)
                outputColumnNames: _col0, _col1, _col2, _col3, _col4, _col5, _col6, _col7, _col8
                Statistics: Num rows: 1 Data size: 0 Basic stats: PARTIAL Column stats: NONE
                Reduce Output Operator
                  key expressions: _col0 (type: string)
                  null sort order: a
                  sort order: +
                  Map-reduce partition columns: _col0 (type: string)
                  Statistics: Num rows: 1 Data size: 0 Basic stats: PARTIAL Column stats: NONE
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
              numRows 0
              rawDataSize 0
              serialization.ddl struct external_customer_table_1 { string customer_id, string customer_fname, string customer_lname, string username, string password, string address, string city, string state, string pincode}
              serialization.format ,
              serialization.lib org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
              totalSize 0
              transient_lastDdlTime 1766057035
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
                numRows 0
                rawDataSize 0
                serialization.ddl struct external_customer_table_1 { string customer_id, string customer_fname, string customer_lname, string username, string password, string address, string city, string state, string pincode}
                serialization.format ,
                serialization.lib org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
                totalSize 0
                transient_lastDdlTime 1766057035
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
              numRows 0
              rawDataSize 0
              serialization.ddl struct external_orders_table_1 { string order_id, string order_date, string customer_id, string order_status}
              serialization.format ,
              serialization.lib org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
              totalSize 0
              transient_lastDdlTime 1766057368
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
                numRows 0
                rawDataSize 0
                serialization.ddl struct external_orders_table_1 { string order_id, string order_date, string customer_id, string order_status}
                serialization.format ,
                serialization.lib org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
                totalSize 0
                transient_lastDdlTime 1766057368
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
          Statistics: Num rows: 1 Data size: 0 Basic stats: PARTIAL Column stats: NONE
          Limit
            Number of rows: 5
            Statistics: Num rows: 1 Data size: 0 Basic stats: PARTIAL Column stats: NONE
            File Output Operator
              compressed: false
              GlobalTableId: 0
              directory: hdfs://m01.itversity.com:9000/tmp/hive/itv020752/itv020752/35460c0c-d847-4455-9983-72de12b79b19/hive_2025-12-18_06-42-34_852_3595931145515627896-2/-mr-10001/.hive-staging_hive_2025-12-18_06-42-34_852_3595931145515627896-2/-ext-10002
              NumFilesPerFileSink: 1
              Statistics: Num rows: 1 Data size: 0 Basic stats: PARTIAL Column stats: NONE
              Stats Publishing Key Prefix: hdfs://m01.itversity.com:9000/tmp/hive/itv020752/itv020752/35460c0c-d847-4455-9983-72de12b79b19/hive_2025-12-18_06-42-34_852_3595931145515627896-2/-mr-10001/.hive-staging_hive_2025-12-18_06-42-34_852_3595931145515627896-2/-ext-10002/
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

Time taken: 40.142 seconds, Fetched: 215 row(s)



###########################################################################################################################

now do this 

hive (misgaurav_hive)> set hive.auto.convert.join = true;
hive (misgaurav_hive)> set hive.enforce.bucketing=true;
hive (misgaurav_hive)> set hive.optimize.bucketmapjoin = true;


hive (misgaurav_hive)> select O.*, C.* from external_orders_table_1 O inner join external_customer_table_1 C
                     > on O.customer_id = C.customer_id limit 5;
Query ID = itv020752_20251218064740_068e8a16-701e-4d3a-8630-0b02112dcdff
Total jobs = 1
SLF4J: Found binding in [jar:file:/opt/apache-hive-3.1.2-bin/lib/log4j-slf4j-impl-2.10.0.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/opt/hadoop-3.3.0/share/hadoop/common/lib/slf4j-log4j12-1.7.25.jar!/org/slf4j/impl/StaticLoggerBinder.cla
ss]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
2025-12-18 06:48:32     Starting to launch local task to process map join;      maximum memory = 14316732416
2025-12-18 06:48:33     Uploaded 1 File to: file:/tmp/itv020752/35460c0c-d847-4455-9983-72de12b79b19/hive_2025-12-18_06-47-40_767_765690925
9492445861-2/-local-10004/HashTable-Stage-3/MapJoin-mapfile00--.hashtable (260 bytes)
2025-12-18 06:48:33     End of local task; Time Taken: 0.872 sec.
Execution completed successfully
MapredLocal task succeeded
Launching Job 1 out of 1
Number of reduce tasks is set to 0 since there's no reduce operator
Starting Job = job_1766025126719_0209, Tracking URL = http://m02.itversity.com:19088/proxy/application_1766025126719_0209/
Kill Command = /opt/hadoop/bin/mapred job  -kill job_1766025126719_0209
Hadoop job information for Stage-3: number of mappers: 0; number of reducers: 0
2025-12-18 06:48:42,946 Stage-3 map = 0%,  reduce = 0%
Ended Job = job_1766025126719_0209
MapReduce Jobs Launched: 
Stage-Stage-3:  HDFS Read: 0 HDFS Write: 0 SUCCESS
Total MapReduce CPU Time Spent: 0 msec
OK
Time taken: 64.333 seconds


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
        $hdt$_0:o 
          Fetch Operator
            limit: -1
      Alias -> Map Local Operator Tree:
        $hdt$_0:o 
          TableScan
            alias: o
            Statistics: Num rows: 1 Data size: 0 Basic stats: PARTIAL Column stats: NONE
            GatherStats: false
            Filter Operator
              isSamplingPred: false
              predicate: customer_id is not null (type: boolean)
              Statistics: Num rows: 1 Data size: 0 Basic stats: PARTIAL Column stats: NONE
              Select Operator
                expressions: order_id (type: string), order_date (type: string), customer_id (type: string), order_status (type: string)
                outputColumnNames: _col0, _col1, _col2, _col3
                Statistics: Num rows: 1 Data size: 0 Basic stats: PARTIAL Column stats: NONE
                HashTable Sink Operator
                  keys:
                    0 _col2 (type: string)
                    1 _col0 (type: string)
                  Position of Big Table: 1

  Stage: Stage-3
    Map Reduce
      Map Operator Tree:
          TableScan
            alias: c
            Statistics: Num rows: 1 Data size: 0 Basic stats: PARTIAL Column stats: NONE
            GatherStats: false
            Filter Operator
              isSamplingPred: false
              predicate: customer_id is not null (type: boolean)
              Statistics: Num rows: 1 Data size: 0 Basic stats: PARTIAL Column stats: NONE
              Select Operator
                expressions: customer_id (type: string), customer_fname (type: string), customer_lname (type: string), username (type: string), password (type: string), address (type: string), city (type: string), state (type: string), pincode (type: string)
                outputColumnNames: _col0, _col1, _col2, _col3, _col4, _col5, _col6, _col7, _col8
                Statistics: Num rows: 1 Data size: 0 Basic stats: PARTIAL Column stats: NONE
                Map Join Operator
                  condition map:
                       Inner Join 0 to 1
                  keys:
                    0 _col2 (type: string)
                    1 _col0 (type: string)
                  outputColumnNames: _col0, _col1, _col2, _col3, _col4, _col5, _col6, _col7, _col8, _col9, _col10, _col11, _col12
                  Position of Big Table: 1
                  Statistics: Num rows: 1 Data size: 0 Basic stats: PARTIAL Column stats: NONE
                  Limit
                    Number of rows: 5
                    Statistics: Num rows: 1 Data size: 0 Basic stats: PARTIAL Column stats: NONE
                    File Output Operator
                      compressed: false
                      GlobalTableId: 0
                      directory: hdfs://m01.itversity.com:9000/tmp/hive/itv020752/itv020752/35460c0c-d847-4455-9983-72de12b79b19/hive_2025-12-18_06-49-59_338_7228754945799300022-2/-mr-10001/.hive-staging_hive_2025-12-18_06-49-59_338_7228754945799300022-2/-ext-10002
                      NumFilesPerFileSink: 1
                      Statistics: Num rows: 1 Data size: 0 Basic stats: PARTIAL Column stats: NONE
                      Stats Publishing Key Prefix: hdfs://m01.itversity.com:9000/tmp/hive/itv020752/itv020752/35460c0c-d847-4455-9983-72de12b79b19/hive_2025-12-18_06-49-59_338_7228754945799300022-2/-mr-10001/.hive-staging_hive_2025-12-18_06-49-59_338_7228754945799300022-2/-ext-10002/
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
        hdfs://m01.itversity.com:9000/user/itv020752/warehouse/misgaurav_hive.db/external_customer_table_1 [$hdt$_1:c]
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
              numRows 0
              rawDataSize 0
              serialization.ddl struct external_customer_table_1 { string customer_id, string customer_fname, string customer_lname, string username, string password, string address, string city, string state, string pincode}
              serialization.format ,
              serialization.lib org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
              totalSize 0
              transient_lastDdlTime 1766057035
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
                numRows 0
                rawDataSize 0
                serialization.ddl struct external_customer_table_1 { string customer_id, string customer_fname, string customer_lname, string username, string password, string address, string city, string state, string pincode}
                serialization.format ,
                serialization.lib org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
                totalSize 0
                transient_lastDdlTime 1766057035
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
              numRows 0
              rawDataSize 0
              serialization.ddl struct external_orders_table_1 { string order_id, string order_date, string customer_id, string order_status}
              serialization.format ,
              serialization.lib org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
              totalSize 0
              transient_lastDdlTime 1766057368
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
                numRows 0
                rawDataSize 0
                serialization.ddl struct external_orders_table_1 { string order_id, string order_date, string customer_id, string order_status}
                serialization.format ,
                serialization.lib org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
                totalSize 0
                transient_lastDdlTime 1766057368
              serde: org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
              name: misgaurav_hive.external_orders_table_1
            name: misgaurav_hive.external_orders_table_1
      Truncated Path -> Alias:
        hdfs://m01.itversity.com:9000/user/itv020752/warehouse/misgaurav_hive.db/external_customer_table_1 [$hdt$_1:c]

  Stage: Stage-0
    Fetch Operator
      limit: 5
      Processor Tree:
        ListSink

Time taken: 52.692 seconds, Fetched: 212 row(s)
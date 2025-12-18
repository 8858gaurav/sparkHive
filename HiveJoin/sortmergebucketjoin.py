# no of bucket should be same on both the tables.
# both the table should be sorted on the joining columns.
# here 1st bucket of table 1 will join with 1st bucket of table 2 and so on.
# since, there are 4 buckets in both the tables, for each bucket join, there would be 1 mapper.
# there is no need to load the data into memory as in case of map side join.
# directly bucket can be join, 1 to 1 maping is happening between the buckets of both the tables.

hive (misgaurav_hive)> SET hive.input.format=org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;
hive (misgaurav_hive)> set hive.autoconvert.sortmerge.join = true;
hive (misgaurav_hive)> set hive.optimize.bucketmapjoin = true;
hive (misgaurav_hive)> set hive.optimize.bucketmapjoin.sortedmerge = true;
hive (misgaurav_hive)> set hive.enforce.bucketing = true;
hive (misgaurav_hive)> set hive.enforce.sorting = true;
hive (misgaurav_hive)> set hive.auto.convert.join = true;

hive (misgaurav_hive)> CREATE EXTERNAL TABLE IF NOT EXISTS misgaurav_hive.external_customer_table_2 (
                     > customer_id STRING,
                     > customer_fname STRING,
                     > customer_lname STRING,
                     > username STRING,
                     > password STRING,
                     > address STRING,
                     > city STRING,
                     > state STRING,
                     > pincode STRING
                     > ) 
                     > CLUSTERED BY (customer_id) 
                     > SORTED BY (customer_id ASC) into 4 buckets
                     > ROW FORMAT DELIMITED
                     > FIELDS TERMINATED BY ','
                     > STORED AS textfile;
OK
Time taken: 0.249 seconds

!hadoop fs -ls -h /user/itv020752/warehouse/misgaurav_hive.db/external_customer_table_2
# found 0 items

hive (misgaurav_hive)>  CREATE EXTERNAL TABLE IF NOT EXISTS misgaurav_hive.external_orders_table_2 (
                     >  order_id STRING,
                     >  order_date STRING,
                     >  customer_id STRING,
                     >  order_status STRING
                     >  ) 
                     >  CLUSTERED BY (customer_id)
                     >  SORTED BY (customer_id ASC) into 4 buckets
                     >  ROW FORMAT DELIMITED
                     >  FIELDS TERMINATED BY ','
                     >  STORED AS textfile;
OK
Time taken: 0.213 seconds

!hadoop fs -ls -h /user/itv020752/warehouse/misgaurav_hive.db/external_orders_table_2
# found 0 items

import re
f = []
for i in spark.catalog.listTables("misgaurav_hive"):
    if re.search(r'external', i.name):
        f.append(i)
print(f)
# [Table(name='external_customer_table', database='misgaurav_hive', description=None, tableType='EXTERNAL', isTemporary=False),
#  Table(name='external_customer_table_1', database='misgaurav_hive', description=None, tableType='EXTERNAL', isTemporary=False),
#  Table(name='external_customer_table_2', database='misgaurav_hive', description=None, tableType='EXTERNAL', isTemporary=False), 
#  Table(name='external_orders_table', database='misgaurav_hive', description=None, tableType='EXTERNAL', isTemporary=False),
#  Table(name='external_orders_table_1', database='misgaurav_hive', description=None, tableType='EXTERNAL', isTemporary=False),
#  Table(name='external_orders_table_2', database='misgaurav_hive', description=None, tableType='EXTERNAL', isTemporary=False)]

!hadoop fs -ls -h /user/itv020752/warehouse/misgaurav_hive.db/
# Found 4 items
# drwxr-xr-x   - itv020752 supergroup          0 2025-12-18 12:57 /user/itv020752/warehouse/misgaurav_hive.db/external_customer_table_1
# drwxr-xr-x   - itv020752 supergroup          0 2025-12-18 13:38 /user/itv020752/warehouse/misgaurav_hive.db/external_customer_table_2
# drwxr-xr-x   - itv020752 supergroup          0 2025-12-18 13:01 /user/itv020752/warehouse/misgaurav_hive.db/external_orders_table_1
# drwxr-xr-x   - itv020752 supergroup          0 2025-12-18 13:41 /user/itv020752/warehouse/misgaurav_hive.db/external_orders_table_2


hive (misgaurav_hive)> show tables;
OK
external_customer_table
external_customer_table_1
external_customer_table_2
external_orders_table
external_orders_table_1
external_orders_table_2
Time taken: 0.025 seconds, Fetched: 6 row(s)

hive (misgaurav_hive)> INSERT INTO misgaurav_hive.external_customer_table_2 select * from external_customer_table;
Query ID = itv020752_20251218133720_d39b7f20-bc61-42a8-b1de-0159a236853b
Total jobs = 2
Launching Job 1 out of 2
Number of reduce tasks determined at compile time: 4
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1766025126719_0383, Tracking URL = http://m02.itversity.com:19088/proxy/application_1766025126719_0383/
Kill Command = /opt/hadoop/bin/mapred job  -kill job_1766025126719_0383
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 4
2025-12-18 13:38:10,007 Stage-1 map = 0%,  reduce = 0%
2025-12-18 13:38:15,165 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 2.36 sec
2025-12-18 13:38:20,290 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 11.23 sec
MapReduce Total cumulative CPU time: 11 seconds 230 msec
Ended Job = job_1766025126719_0383
Loading data to table misgaurav_hive.external_customer_table_2
Launching Job 2 out of 2
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1766025126719_0384, Tracking URL = http://m02.itversity.com:19088/proxy/application_1766025126719_0384/
Kill Command = /opt/hadoop/bin/mapred job  -kill job_1766025126719_0384
Hadoop job information for Stage-3: number of mappers: 4; number of reducers: 1
2025-12-18 13:38:31,525 Stage-3 map = 0%,  reduce = 0%
2025-12-18 13:38:35,704 Stage-3 map = 50%,  reduce = 0%, Cumulative CPU 2.6 sec
2025-12-18 13:38:36,758 Stage-3 map = 100%,  reduce = 0%, Cumulative CPU 5.31 sec
2025-12-18 13:38:40,866 Stage-3 map = 100%,  reduce = 100%, Cumulative CPU 7.16 sec
MapReduce Total cumulative CPU time: 7 seconds 160 msec
Ended Job = job_1766025126719_0384
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 1  Reduce: 4   Cumulative CPU: 11.23 sec   HDFS Read: 1007195 HDFS Write: 972127 SUCCESS
Stage-Stage-3: Map: 4  Reduce: 1   Cumulative CPU: 7.16 sec   HDFS Read: 56875 HDFS Write: 6065 SUCCESS
Total MapReduce CPU Time Spent: 18 seconds 390 msec
OK
Time taken: 82.893 seconds

!hadoop fs -ls -h /user/itv020752/warehouse/misgaurav_hive.db/external_customer_table_2
# Found 4 items
# -rw-r--r--   3 itv020752 supergroup    232.5 K 2025-12-18 13:38 /user/itv020752/warehouse/misgaurav_hive.db/external_customer_table_2/000000_0
# -rw-r--r--   3 itv020752 supergroup    233.5 K 2025-12-18 13:38 /user/itv020752/warehouse/misgaurav_hive.db/external_customer_table_2/000001_0
# -rw-r--r--   3 itv020752 supergroup    232.4 K 2025-12-18 13:38 /user/itv020752/warehouse/misgaurav_hive.db/external_customer_table_2/000002_0
# -rw-r--r--   3 itv020752 supergroup    232.9 K 2025-12-18 13:38 /user/itv020752/warehouse/misgaurav_hive.db/external_customer_table_2/000003_0


hive (misgaurav_hive)> INSERT INTO misgaurav_hive.external_orders_table_2 select * from external_orders_table;
Query ID = itv020752_20251218133916_41e9d7ca-7e1f-40f1-8618-eafa33df005c
Total jobs = 2
Launching Job 1 out of 2
Number of reduce tasks determined at compile time: 4
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1766025126719_0385, Tracking URL = http://m02.itversity.com:19088/proxy/application_1766025126719_0385/
Kill Command = /opt/hadoop/bin/mapred job  -kill job_1766025126719_0385
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 4
2025-12-18 13:41:07,403 Stage-1 map = 0%,  reduce = 0%
2025-12-18 13:41:12,554 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 2.96 sec
2025-12-18 13:41:16,636 Stage-1 map = 100%,  reduce = 50%, Cumulative CPU 7.68 sec
2025-12-18 13:41:17,655 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 12.21 sec
MapReduce Total cumulative CPU time: 12 seconds 210 msec
Ended Job = job_1766025126719_0385
Loading data to table misgaurav_hive.external_orders_table_2
Launching Job 2 out of 2
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1766025126719_0386, Tracking URL = http://m02.itversity.com:19088/proxy/application_1766025126719_0386/
Kill Command = /opt/hadoop/bin/mapred job  -kill job_1766025126719_0386
Hadoop job information for Stage-3: number of mappers: 4; number of reducers: 1
2025-12-18 13:41:28,509 Stage-3 map = 0%,  reduce = 0%
2025-12-18 13:41:33,651 Stage-3 map = 100%,  reduce = 0%, Cumulative CPU 5.29 sec
2025-12-18 13:41:38,775 Stage-3 map = 100%,  reduce = 100%, Cumulative CPU 6.82 sec
MapReduce Total cumulative CPU time: 6 seconds 820 msec
Ended Job = job_1766025126719_0386
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 1  Reduce: 4   Cumulative CPU: 12.21 sec   HDFS Read: 3039886 HDFS Write: 3008959 SUCCESS
Stage-Stage-3: Map: 4  Reduce: 1   Cumulative CPU: 6.82 sec   HDFS Read: 36929 HDFS Write: 2996 SUCCESS
Total MapReduce CPU Time Spent: 19 seconds 30 msec
OK
Time taken: 143.253 seconds

!hadoop fs -ls -h /user/itv020752/warehouse/misgaurav_hive.db/external_orders_table_2
# Found 4 items
# -rw-r--r--   3 itv020752 supergroup    730.5 K 2025-12-18 13:41 /user/itv020752/warehouse/misgaurav_hive.db/external_orders_table_2/000000_0
# -rw-r--r--   3 itv020752 supergroup    729.4 K 2025-12-18 13:41 /user/itv020752/warehouse/misgaurav_hive.db/external_orders_table_2/000001_0
# -rw-r--r--   3 itv020752 supergroup    730.6 K 2025-12-18 13:41 /user/itv020752/warehouse/misgaurav_hive.db/external_orders_table_2/000002_0
# -rw-r--r--   3 itv020752 supergroup    739.2 K 2025-12-18 13:41 /user/itv020752/warehouse/misgaurav_hive.db/external_orders_table_2/000003_0

hive (misgaurav_hive)> select O.*, C.* from external_orders_table_2 O inner join external_customer_table_2 C
                     >                      on O.customer_id = C.customer_id limit 5;
Query ID = itv020752_20251218134324_646885d7-b76e-43e3-808d-d32c8397c499
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks is set to 0 since there's no reduce operator
Starting Job = job_1766025126719_0388, Tracking URL = http://m02.itversity.com:19088/proxy/application_1766025126719_0388/
Kill Command = /opt/hadoop/bin/mapred job  -kill job_1766025126719_0388
Hadoop job information for Stage-1: number of mappers: 4; number of reducers: 0
2025-12-18 13:44:18,710 Stage-1 map = 0%,  reduce = 0%
2025-12-18 13:44:23,878 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 12.84 sec
MapReduce Total cumulative CPU time: 12 seconds 840 msec
Ended Job = job_1766025126719_0388
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 4   Cumulative CPU: 12.84 sec   HDFS Read: 3063592 HDFS Write: 3066 SUCCESS
Total MapReduce CPU Time Spent: 12 seconds 840 msec
OK
59261   2013-09-23 00:00:00.0   10003   COMPLETE        10003   Robert  Kerr    XXXXXXXXX       XXXXXXXXX       4948 Colonial Village   Cag
uas     PR      00725
31736   2014-02-06 00:00:00.0   10003   COMPLETE        10003   Robert  Kerr    XXXXXXXXX       XXXXXXXXX       4948 Colonial Village   Cag
uas     PR      00725
40860   2014-04-02 00:00:00.0   10003   PENDING_PAYMENT 10003   Robert  Kerr    XXXXXXXXX       XXXXXXXXX       4948 Colonial Village   Cag
uas     PR      00725
11586   2013-10-04 00:00:00.0   10003   PENDING_PAYMENT 10003   Robert  Kerr    XXXXXXXXX       XXXXXXXXX       4948 Colonial Village   Cag
uas     PR      00725
68294   2014-03-31 00:00:00.0   10003   PENDING_PAYMENT 10003   Robert  Kerr    XXXXXXXXX       XXXXXXXXX       4948 Colonial Village   Cag
uas     PR      00725
Time taken: 61.629 seconds, Fetched: 5 row(s)

######################### see line no : 244 ##############


hive (misgaurav_hive)> explain extended select O.*, C.* from external_orders_table_2 O inner join external_customer_table_2 C
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
                Sorted Merge Bucket Map Join Operator
                  condition map:
                       Inner Join 0 to 1
                  keys:
                    0 _col2 (type: string)
                    1 _col0 (type: string)
                  outputColumnNames: _col0, _col1, _col2, _col3, _col4, _col5, _col6, _col7, _col8, _col9, _col10, _col11, _col12
                  Position of Big Table: 0
                  BucketMapJoin: true
                  Limit
                    Number of rows: 5
                    File Output Operator
                      compressed: false
                      GlobalTableId: 0
                      directory: hdfs://m01.itversity.com:9000/tmp/hive/itv020752/itv020752/72fa5fea-f7de-48fc-921f-e3ac6a8771a3/hive_2025-12-18_13-46-06_635_4449741593854095842-2/-mr-10001/.hive-staging_hive_2025-12-18_13-46-06_635_4449741593854095842-2/-ext-10002
                      NumFilesPerFileSink: 1
                      Stats Publishing Key Prefix: hdfs://m01.itversity.com:9000/tmp/hive/itv020752/itv020752/72fa5fea-f7de-48fc-921f-e3ac6a8771a3/hive_2025-12-18_13-46-06_635_4449741593854095842-2/-mr-10001/.hive-staging_hive_2025-12-18_13-46-06_635_4449741593854095842-2/-ext-10002/
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
      Path -> Alias:
        hdfs://m01.itversity.com:9000/user/itv020752/warehouse/misgaurav_hive.db/external_orders_table_2 [$hdt$_0:o]
      Path -> Partition:
        hdfs://m01.itversity.com:9000/user/itv020752/warehouse/misgaurav_hive.db/external_orders_table_2 
          Partition
            base file name: external_orders_table_2
            input format: org.apache.hadoop.mapred.TextInputFormat
            output format: org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
            properties:
              COLUMN_STATS_ACCURATE {"BASIC_STATS":"true","COLUMN_STATS":{"customer_id":"true","order_date":"true","order_id":"true","order_status":"true"}}
              EXTERNAL TRUE
              SORTBUCKETCOLSPREFIX TRUE
              bucket_count 4
              bucket_field_name customer_id
              bucketing_version 2
              column.name.delimiter ,
              columns order_id,order_date,customer_id,order_status
              columns.comments 
              columns.types string:string:string:string
              field.delim ,
              file.inputformat org.apache.hadoop.mapred.TextInputFormat
              file.outputformat org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
              location hdfs://m01.itversity.com:9000/user/itv020752/warehouse/misgaurav_hive.db/external_orders_table_2
              name misgaurav_hive.external_orders_table_2
              numFiles 4
              numRows 68883
              rawDataSize 2931061
              serialization.ddl struct external_orders_table_2 { string order_id, string order_date, string customer_id, string order_status}
              serialization.format ,
              serialization.lib org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
              totalSize 2999944
              transient_lastDdlTime 1766083300
            serde: org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
          
              input format: org.apache.hadoop.mapred.TextInputFormat
              output format: org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
              properties:
                COLUMN_STATS_ACCURATE {"BASIC_STATS":"true","COLUMN_STATS":{"customer_id":"true","order_date":"true","order_id":"true","order_status":"true"}}
                EXTERNAL TRUE
                SORTBUCKETCOLSPREFIX TRUE
                bucket_count 4
                bucket_field_name customer_id
                bucketing_version 2
                column.name.delimiter ,
                columns order_id,order_date,customer_id,order_status
                columns.comments 
                columns.types string:string:string:string
                field.delim ,
                file.inputformat org.apache.hadoop.mapred.TextInputFormat
                file.outputformat org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
                location hdfs://m01.itversity.com:9000/user/itv020752/warehouse/misgaurav_hive.db/external_orders_table_2
                name misgaurav_hive.external_orders_table_2
                numFiles 4
                numRows 68883
                rawDataSize 2931061
                serialization.ddl struct external_orders_table_2 { string order_id, string order_date, string customer_id, string order_status}
                serialization.format ,
                serialization.lib org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
                totalSize 2999944
                transient_lastDdlTime 1766083300
              serde: org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
              name: misgaurav_hive.external_orders_table_2
            name: misgaurav_hive.external_orders_table_2
      Truncated Path -> Alias:
        hdfs://m01.itversity.com:9000/user/itv020752/warehouse/misgaurav_hive.db/external_orders_table_2 [$hdt$_0:o]

  Stage: Stage-0
    Fetch Operator
      limit: 5
      Processor Tree:
        ListSink

Time taken: 59.306 seconds, Fetched: 124 row(s)
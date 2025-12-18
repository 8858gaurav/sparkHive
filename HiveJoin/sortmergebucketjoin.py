# no of bucket should be same on both the tables.
# both the table should be sorted on the joining columns

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
# Found 4 items
# -rw-r--r--   3 itv020752 supergroup          0 2025-12-18 07:13 /user/itv020752/warehouse/misgaurav_hive.db/external_customer_table_2/000000_0
# -rw-r--r--   3 itv020752 supergroup          0 2025-12-18 07:13 /user/itv020752/warehouse/misgaurav_hive.db/external_customer_table_2/000001_0
# -rw-r--r--   3 itv020752 supergroup          0 2025-12-18 07:13 /user/itv020752/warehouse/misgaurav_hive.db/external_customer_table_2/000002_0
# -rw-r--r--   3 itv020752 supergroup          0 2025-12-18 07:13 /user/itv020752/warehouse/misgaurav_hive.db/external_customer_table_2/000003_0

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
# Found 4 items
# -rw-r--r--   3 itv020752 supergroup          0 2025-12-18 07:16 /user/itv020752/warehouse/misgaurav_hive.db/external_orders_table_2/000000_0
# -rw-r--r--   3 itv020752 supergroup          0 2025-12-18 07:16 /user/itv020752/warehouse/misgaurav_hive.db/external_orders_table_2/000001_0
# -rw-r--r--   3 itv020752 supergroup          0 2025-12-18 07:16 /user/itv020752/warehouse/misgaurav_hive.db/external_orders_table_2/000002_0
# -rw-r--r--   3 itv020752 supergroup          0 2025-12-18 07:16 /user/itv020752/warehouse/misgaurav_hive.db/external_orders_table_2/000003_0

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
Query ID = itv020752_20251218071247_9af08d67-da91-408f-b70d-629dabca2071
Total jobs = 2
Launching Job 1 out of 2
Number of reduce tasks determined at compile time: 4
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1766025126719_0218, Tracking URL = http://m02.itversity.com:19088/proxy/application_1766025126719_0218/
Kill Command = /opt/hadoop/bin/mapred job  -kill job_1766025126719_0218
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 4
2025-12-18 07:13:32,284 Stage-1 map = 0%,  reduce = 0%
2025-12-18 07:13:37,420 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 1.3 sec
2025-12-18 07:13:41,528 Stage-1 map = 100%,  reduce = 75%, Cumulative CPU 6.7 sec
2025-12-18 07:13:42,546 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 8.46 sec
MapReduce Total cumulative CPU time: 8 seconds 460 msec
Ended Job = job_1766025126719_0218
Loading data to table misgaurav_hive.external_customer_table_2
Launching Job 2 out of 2
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1766025126719_0219, Tracking URL = http://m02.itversity.com:19088/proxy/application_1766025126719_0219/
Kill Command = /opt/hadoop/bin/mapred job  -kill job_1766025126719_0219
Hadoop job information for Stage-3: number of mappers: 4; number of reducers: 1
2025-12-18 07:13:53,399 Stage-3 map = 0%,  reduce = 0%
2025-12-18 07:13:58,546 Stage-3 map = 100%,  reduce = 0%, Cumulative CPU 5.81 sec
2025-12-18 07:14:03,678 Stage-3 map = 100%,  reduce = 100%, Cumulative CPU 7.45 sec
MapReduce Total cumulative CPU time: 7 seconds 450 msec
Ended Job = job_1766025126719_0219
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 1  Reduce: 4   Cumulative CPU: 8.46 sec   HDFS Read: 53665 HDFS Write: 1332 SUCCESS
Stage-Stage-3: Map: 4  Reduce: 1   Cumulative CPU: 7.45 sec   HDFS Read: 39936 HDFS Write: 262 SUCCESS
Total MapReduce CPU Time Spent: 15 seconds 910 msec
OK
Time taken: 78.534 seconds

hive (misgaurav_hive)> INSERT INTO misgaurav_hive.external_orders_table_2 select * from external_orders_table;
Query ID = itv020752_20251218071545_79559dfe-b4c4-41a7-bfcc-a09192ab94f3
Total jobs = 2
Launching Job 1 out of 2
Number of reduce tasks determined at compile time: 4
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1766025126719_0221, Tracking URL = http://m02.itversity.com:19088/proxy/application_1766025126719_0221/
Kill Command = /opt/hadoop/bin/mapred job  -kill job_1766025126719_0221
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 4
2025-12-18 07:16:40,670 Stage-1 map = 0%,  reduce = 0%
2025-12-18 07:16:44,803 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 1.21 sec
2025-12-18 07:16:48,911 Stage-1 map = 100%,  reduce = 25%, Cumulative CPU 3.03 sec
2025-12-18 07:16:49,939 Stage-1 map = 100%,  reduce = 75%, Cumulative CPU 6.88 sec
2025-12-18 07:16:50,970 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 8.9 sec
MapReduce Total cumulative CPU time: 8 seconds 900 msec
Ended Job = job_1766025126719_0221
Loading data to table misgaurav_hive.external_orders_table_2
Launching Job 2 out of 2
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1766025126719_0222, Tracking URL = http://m02.itversity.com:19088/proxy/application_1766025126719_0222/
Kill Command = /opt/hadoop/bin/mapred job  -kill job_1766025126719_0222
Hadoop job information for Stage-3: number of mappers: 4; number of reducers: 1
2025-12-18 07:17:02,993 Stage-3 map = 0%,  reduce = 0%
2025-12-18 07:17:08,167 Stage-3 map = 100%,  reduce = 0%, Cumulative CPU 5.94 sec
2025-12-18 07:17:13,297 Stage-3 map = 100%,  reduce = 100%, Cumulative CPU 7.4 sec
MapReduce Total cumulative CPU time: 7 seconds 400 msec
Ended Job = job_1766025126719_0222
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 1  Reduce: 4   Cumulative CPU: 8.9 sec   HDFS Read: 40198 HDFS Write: 980 SUCCESS
Stage-Stage-3: Map: 4  Reduce: 1   Cumulative CPU: 7.4 sec   HDFS Read: 29042 HDFS Write: 171 SUCCESS
Total MapReduce CPU Time Spent: 16 seconds 300 msec
OK
Time taken: 90.574 seconds

hive (misgaurav_hive)> select O.*, C.* from external_orders_table_2 O inner join external_customer_table_2 C
                     > on O.customer_id = C.customer_id limit 5;
Query ID = itv020752_20251218072258_47afd760-d738-47f2-9d93-c0be456e91aa
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks is set to 0 since there's no reduce operator
Starting Job = job_1766025126719_0225, Tracking URL = http://m02.itversity.com:19088/proxy/application_1766025126719_0225/
Kill Command = /opt/hadoop/bin/mapred job  -kill job_1766025126719_0225
Hadoop job information for Stage-1: number of mappers: 4; number of reducers: 0
2025-12-18 07:24:00,719 Stage-1 map = 0%,  reduce = 0%
2025-12-18 07:24:04,791 Stage-1 map = 25%,  reduce = 0%, Cumulative CPU 2.12 sec
2025-12-18 07:24:05,806 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 7.85 sec
MapReduce Total cumulative CPU time: 7 seconds 850 msec
Ended Job = job_1766025126719_0225
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 4   Cumulative CPU: 7.85 sec   HDFS Read: 46808 HDFS Write: 348 SUCCESS
Total MapReduce CPU Time Spent: 7 seconds 850 msec
OK
Time taken: 68.164 seconds

######################### see line no : 188 ##############


hive (misgaurav_hive)> explain extended select O.*, C.* from external_orders_table_2 O inner join external_customer_table_2 C
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
                      directory: hdfs://m01.itversity.com:9000/tmp/hive/itv020752/itv020752/35460c0c-d847-4455-9983-72de12b79b19/hive_2025-12-18_07-26-20_432_650896582914831591-2/-mr-10001/.hive-staging_hive_2025-12-18_07-26-20_432_650896582914831591-2/-ext-10002
                      NumFilesPerFileSink: 1
                      Stats Publishing Key Prefix: hdfs://m01.itversity.com:9000/tmp/hive/itv020752/itv020752/35460c0c-d847-4455-9983-72de12b79b19/hive_2025-12-18_07-26-20_432_650896582914831591-2/-mr-10001/.hive-staging_hive_2025-12-18_07-26-20_432_650896582914831591-2/-ext-10002/
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
              numRows 0
              rawDataSize 0
              serialization.ddl struct external_orders_table_2 { string order_id, string order_date, string customer_id, string order_status}
              serialization.format ,
              serialization.lib org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
              totalSize 0
              transient_lastDdlTime 1766060235
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
                numRows 0
                rawDataSize 0
                serialization.ddl struct external_orders_table_2 { string order_id, string order_date, string customer_id, string order_status}
                serialization.format ,
                serialization.lib org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
                totalSize 0
                transient_lastDdlTime 1766060235
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

Time taken: 54.965 seconds, Fetched: 124 row(s)

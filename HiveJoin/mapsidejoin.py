hive> use misgaurav_hive;
OK
Time taken: 3.425 seconds
# to show the current db in hive prompt
hive> set hive.cli.print.current.db = true;
hive (misgaurav_hive)> show tables;
OK
external_customer_table
external_orders_table
Time taken: 0.056 seconds, Fetched: 2 row(s)
hive (misgaurav_hive)> 

hive (misgaurav_hive)> set hive.auto.convert.join = true;

#########################################

In Apache Hive, the command set hive.auto.convert.join = true; is a performance optimization setting that enables Map-Side Joins (also known as Broadcast Joins).
What it Does
By default, Hive often performs joins using a Reduce-Side Join. This involves shuffling data across the network to reducers, which is resource-intensive and slow.
When you set this property to true, Hive attempts to optimize the query by:
Checking if one of the tables in your join is small enough to fit into memory (RAM). hash table will be created
If it fits, Hive broadcasts that small table to all worker nodes.
The join is then performed locally on each map task, completely skipping the "Reduce" phase.

##########################################

hive (misgaurav_hive)> set hive.mapjoin.smalltable.filesize;
hive.mapjoin.smalltable.filesize=25000000
## if table < 25 mb, then it will automatically trigger the broadcast or map side join, once we enable map side join.

hive (misgaurav_hive)> select O.*, C.* from external_orders_table O inner join external_customer_table C
                     >                      on O.customer_id = C.customer_id limit 5;
Query ID = itv020752_20251218123552_d4b1076e-f164-4eee-b553-ddd53646edff
Total jobs = 1
SLF4J: Found binding in [jar:file:/opt/apache-hive-3.1.2-bin/lib/log4j-slf4j-impl-2.10.0.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/opt/hadoop-3.3.0/share/hadoop/common/lib/slf4j-log4j12-1.7.25.jar!/org/slf4j/impl/StaticLoggerBinder.cla
ss]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.SLF4J: Actual binding is of type [org.apache.logging.slf4j.
Log4jLoggerFactory]
2025-12-18 12:36:44     Starting to launch local task to process map join;      maximum memory = 14316732416
2025-12-18 12:36:45     Dump the side-table for tag: 1 with group count: 12435 into file: file:/tmp/itv020752/72fa5fea-f7de-48fc-921f-e3ac6
a8771a3/hive_2025-12-18_12-35-52_814_4631105923505185333-2/-local-10004/HashTable-Stage-3/MapJoin-mapfile11--.hashtable
Execution completed successfully
MapredLocal task succeeded
Launching Job 1 out of 1
Number of reduce tasks is set to 0 since there's no reduce operator
Starting Job = job_1766025126719_0342, Tracking URL = http://m02.itversity.com:19088/proxy/application_1766025126719_0342/
Kill Command = /opt/hadoop/bin/mapred job  -kill job_1766025126719_0342
Hadoop job information for Stage-3: number of mappers: 1; number of reducers: 0
2025-12-18 12:36:55,027 Stage-3 map = 0%,  reduce = 0%
2025-12-18 12:36:59,168 Stage-3 map = 100%,  reduce = 0%, Cumulative CPU 2.29 sec
MapReduce Total cumulative CPU time: 2 seconds 290 msec
Ended Job = job_1766025126719_0342
MapReduce Jobs Launched: 
Stage-Stage-3: Map: 1   Cumulative CPU: 2.29 sec   HDFS Read: 272974 HDFS Write: 740 SUCCESS
Total MapReduce CPU Time Spent: 2 seconds 290 msec
OK
1       2013-07-25 00:00:00.0   11599   CLOSED  11599   Mary    Malone  XXXXXXXXX       XXXXXXXXX       8708 Indian Horse Highway       Hic
kory    NC      28601
2       2013-07-25 00:00:00.0   256     PENDING_PAYMENT 256     David   Rodriguez       XXXXXXXXX       XXXXXXXXX       7605 Tawny Horse Fa
lls     Chicago IL      60625
3       2013-07-25 00:00:00.0   12111   COMPLETE        12111   Amber   Franco  XXXXXXXXX       XXXXXXXXX       8766 Clear Prairie Line San
ta Cruz CA      95060
4       2013-07-25 00:00:00.0   8827    CLOSED  8827    Brian   Wilson  XXXXXXXXX       XXXXXXXXX       8396 High Corners       San Antonio
TX      78240
5       2013-07-25 00:00:00.0   11318   COMPLETE        11318   Mary    Henry   XXXXXXXXX       XXXXXXXXX       3047 Silent Embers Maze Caguas     PR      00725
Time taken: 67.718 seconds, Fetched: 5 row(s)

######  you'll see map join operators, line no 122 #######

hive (misgaurav_hive)> explain extended select O.*, C.* from external_orders_table O inner join external_customer_table C
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
            Statistics: Num rows: 8798 Data size: 9537190 Basic stats: COMPLETE Column stats: NONE
            GatherStats: false
            Filter Operator
              isSamplingPred: false
              predicate: customer_id is not null (type: boolean)
              Statistics: Num rows: 8798 Data size: 9537190 Basic stats: COMPLETE Column stats: NONE
              Select Operator
                expressions: customer_id (type: string), customer_fname (type: string), customer_lname (type: string), username (type: stri
ng), password (type: string), address (type: string), city (type: string), state (type: string), pincode (type: string)
                outputColumnNames: _col0, _col1, _col2, _col3, _col4, _col5, _col6, _col7, _col8
                Statistics: Num rows: 8798 Data size: 9537190 Basic stats: COMPLETE Column stats: NONE
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
                Map Join Operator
                  condition map:
                       Inner Join 0 to 1
                  keys:
                    0 _col2 (type: string)
                    1 _col0 (type: string)
                  outputColumnNames: _col0, _col1, _col2, _col3, _col4, _col5, _col6, _col7, _col8, _col9, _col10, _col11, _col12
                  Position of Big Table: 0
                  Statistics: Num rows: 56504 Data size: 32999384 Basic stats: COMPLETE Column stats: NONE
                  Limit
                    Number of rows: 5
                    Statistics: Num rows: 5 Data size: 2920 Basic stats: COMPLETE Column stats: NONE
                    File Output Operator
                      compressed: false
                      GlobalTableId: 0
                      directory: hdfs://m01.itversity.com:9000/tmp/hive/itv020752/itv020752/72fa5fea-f7de-48fc-921f-e3ac6a8771a3/hive_2025-12-18_12-38-46_940_450833386239302067-2/-mr-10001/.hive-staging_hive_2025-12-18_12-38-46_940_450833386239302067-2/-ext-10002
                      NumFilesPerFileSink: 1
                      Statistics: Num rows: 5 Data size: 2920 Basic stats: COMPLETE Column stats: NONE
                      Stats Publishing Key Prefix: hdfs://m01.itversity.com:9000/tmp/hive/itv020752/itv020752/72fa5fea-f7de-48fc-921f-e3ac6a8771a3/hive_2025-12-18_12-38-46_940_450833386239302067-2/-mr-10001/.hive-staging_hive_2025-12-18_12-38-46_940_450833386239302067-2/-ext-10002/
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
        hdfs://m01.itversity.com:9000/user/itv020752/hive_datasets/orders [$hdt$_0:o]

  Stage: Stage-0
    Fetch Operator
      limit: 5
      Processor Tree:
        ListSink

Time taken: 51.692 seconds, Fetched: 192 row(s)

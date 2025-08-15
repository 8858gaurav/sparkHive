# hive> create table product (pid int, name string, price int)
#     > clustered by (pid) into 20 buckets stored as orc
#     > TBLPROPERTIES ('transactional' = 'true');

# FAILED: Execution Error, return code 1 from org.apache.hadoop.hive.ql.exec.DDLTask. 
# MetaException(message:Got exception: org.apache.hadoop.security.AccessControlException 
# Permission denied: user=itv020752, access=WRITE, inode="/user/hive/warehouse":hive:students:drwxr-xr-x

# Transaction table (form of delta table): Internal
# ACID Properties in HIVE, when we talked about trxn, it should be databases, then our s/s should handle ACID properties.
# when we are doing Insert/Updates/Delete in Hive, then we need transactions in hive (ACID)
# for creating a transaction table, we can change metadata location to some other, how?
# hive> set hive.metastore.warehouse.dir = /user/itv020752/warehouse;

# Transactional tables in Hive provide Atomicity, Consistency, Isolation, and Durability (ACID) properties, 
# allowing for features like updates, deletes, and merges. To enable ACID, tables must be stored in the ORC file format 
# and have the transactional property set to true

# use this one first
# hive> set hive.metastore.warehouse.dir = /user/itv020752/warehouse;
# to create a db in hive use the above one, we don't have an access to user/hive/warehouse
# hive> SET hive.support.concurrency=true;
# hive> SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
# hive> SET hive.enforce.bucketing=true;
# hive> SET hive.exec.dynamic.partition.mode=nostrict;
# hive> SET hive.compactor.initiator.on=true;
# hive> SET hive.compactor.worker.threads=1

# hive> create database misgaurav_acid;
# OK
# Time taken: 0.233 seconds
# hive> use misgaurav_acid;
# OK
# Time taken: 0.036 seconds
    
# only managed/internal table can be transaction tables, external table can't be.
# only ORC file format it will support while creating a transactional hive tables.

# ext table can't be created to support acid, since the changes on ext table are beyond the control of hive.
# load is not supported in acid transactional tables, insert into will work here only. 
# acid table can't be converted to non-acid table, but vice-versa is true.

# hive> CREATE TABLE IF NOT EXISTS orders_trx1 (
#     > order_id integer,
#     > order_date string,
#     > customer_id integer,
#     > order_status string
#     > )
#     > ROW FORMAT DELIMITED
#     > FIELDS TERMINATED BY ','
#     > STORED AS ORC
#     > TBLPROPERTIES ('transactional'='true');
# OK
# Time taken: 1.252 seconds

# hive> describe formatted orders_trx1;
# OK
# # col_name              data_type               comment             
# order_id                int                                         
# order_date              string                                      
# customer_id             int                                         
# order_status            string                                      
                 
# # Detailed Table Information             
# Database:               misgaurav_acid           
# OwnerType:              USER                     
# Owner:                  itv020752                
# CreateTime:             Thu Aug 14 08:40:18 EDT 2025     
# LastAccessTime:         UNKNOWN                  
# Retention:              0                        
# Location:               hdfs://m01.itversity.com:9000/user/itv020752/warehouse/misgaurav_acid.db/orders_trx1     
# Table Type:             MANAGED_TABLE            
# Table Parameters:                
#         COLUMN_STATS_ACCURATE   {\"BASIC_STATS\":\"true\",\"COLUMN_STATS\":{\"customer_id\":\"true\",\"order_date\":\"true\",\"order_id\":\"true\",\"order_status\":\"true\"}}
#         bucketing_version       2                   
#         numFiles                0                   
#         numRows                 0                   
#         rawDataSize             0                   
#         totalSize               0                   
#         transactional           true                
#         transactional_properties        default             
#         transient_lastDdlTime   1755175218          
                 
# # Storage Information            
# SerDe Library:          org.apache.hadoop.hive.ql.io.orc.OrcSerde        
# InputFormat:            org.apache.hadoop.hive.ql.io.orc.OrcInputFormat  
# OutputFormat:           org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat         
# Compressed:             No                       
# Num Buckets:            -1                       
# Bucket Columns:         []                       
# Sort Columns:           []                       
# Storage Desc Params:             
#         field.delim             ,                   
#         serialization.format    ,                   
# Time taken: 0.649 seconds, Fetched: 37 row(s)

# hive> insert into orders_trx1 values (1,"2013-07-25 00:00:00.0",45,"COMPLETE");
# Query ID = itv020752_20250814084654_a665c589-2ae4-4534-b5fb-cb40fc77f45d
# Total jobs = 1
# Launching Job 1 out of 1
# Number of reduce tasks determined at compile time: 1
# In order to change the average load for a reducer (in bytes):
#   set hive.exec.reducers.bytes.per.reducer=<number>
# In order to limit the maximum number of reducers:
#   set hive.exec.reducers.max=<number>
# In order to set a constant number of reducers:
#   set mapreduce.job.reduces=<number>
# Starting Job = job_1754561546788_3371, Tracking URL = http://m02.itversity.com:19088/proxy/application_1754561546788_3371/
# Kill Command = /opt/hadoop/bin/mapred job  -kill job_1754561546788_3371
# Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
# 2025-08-14 08:47:39,161 Stage-1 map = 0%,  reduce = 0%
# 2025-08-14 08:47:43,329 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 2.64 sec
# 2025-08-14 08:47:47,431 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 4.22 sec
# MapReduce Total cumulative CPU time: 4 seconds 220 msec
# Ended Job = job_1754561546788_3371
# Loading data to table misgaurav_acid.orders_trx1
# MapReduce Jobs Launched: 
# Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 4.22 sec   HDFS Read: 18982 HDFS Write: 1257 SUCCESS
# Total MapReduce CPU Time Spent: 4 seconds 220 msec
# OK
# Time taken: 55.917 seconds

# [itv020752@g01 ~]$ hadoop fs -ls warehouse/misgaurav_acid.db/orders_trx1
# Found 1 items
# drwxr-xr-x   - itv020752 supergroup          0 2025-08-14 08:47 warehouse/misgaurav_acid.db/orders_trx1/delta_0000001_0000001_0000

# [itv020752@g01 ~]$ hadoop fs -ls warehouse/misgaurav_acid.db/orders_trx1/delta_0000001_0000001_0000
# Found 2 items
# -rw-r--r--   3 itv020752 supergroup          1 2025-08-14 08:47 warehouse/misgaurav_acid.db/orders_trx1/delta_0000001_0000001_0000/_orc_acid_version
# -rw-r--r--   3 itv020752 supergroup        942 2025-08-14 08:47 warehouse/misgaurav_acid.db/orders_trx1/delta_0000001_0000001_0000/bucket_00000

# [itv020752@g01 ~]$ hadoop fs -cat warehouse/misgaurav_acid.db/orders_trx1/delta_0000001_0000001_0000/_orc_acid_version ; 2

# hive> select * from orders_trx1;
# OK
# 1       2013-07-25 00:00:00.0   45      COMPLETE
# Time taken: 27.038 seconds, Fetched: 1 row(s)

# update will create 2 directories: 1 for delete, and 1 for insert.


# hive> update orders_trx1 SET order_status = "CLOSED" where order_id=1;
# Query ID = itv020752_20250814091355_bc88f7d3-940a-4ca8-8499-0e84329ad9f4
# Total jobs = 1
# Launching Job 1 out of 1
# Number of reduce tasks not specified. Estimated from input data size: 1
# In order to change the average load for a reducer (in bytes):
#   set hive.exec.reducers.bytes.per.reducer=<number>
# In order to limit the maximum number of reducers:
#   set hive.exec.reducers.max=<number>
# In order to set a constant number of reducers:
#   set mapreduce.job.reduces=<number>
# Starting Job = job_1754561546788_3389, Tracking URL = http://m02.itversity.com:19088/proxy/application_1754561546788_3389/
# Kill Command = /opt/hadoop/bin/mapred job  -kill job_1754561546788_3389
# Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
# 2025-08-14 09:14:04,059 Stage-1 map = 0%,  reduce = 0%
# 2025-08-14 09:14:09,191 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 2.25 sec
# 2025-08-14 09:14:13,350 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 4.32 sec
# MapReduce Total cumulative CPU time: 4 seconds 320 msec
# Ended Job = job_1754561546788_3389
# Loading data to table misgaurav_acid.orders_trx1
# MapReduce Jobs Launched: 
# Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 4.32 sec   HDFS Read: 15280 HDFS Write: 1794 SUCCESS
# Total MapReduce CPU Time Spent: 4 seconds 320 msec
# OK
# Time taken: 19.764 seconds

# internally it's working as :
# delete this records: (1,"2013-07-25 00:00:00.0",45,"COMPLETE")
# insert this records: (1,"2013-07-25 00:00:00.0",45,"CLOSED")

# [itv020752@g01 ~]$ hadoop fs -ls warehouse/misgaurav_acid.db/orders_trx1/
# Found 3 items
# drwxr-xr-x   - itv020752 supergroup          0 2025-08-14 09:14 warehouse/misgaurav_acid.db/orders_trx1/delete_delta_0000002_0000002_0000; what has to be deleted
# drwxr-xr-x   - itv020752 supergroup          0 2025-08-14 08:47 warehouse/misgaurav_acid.db/orders_trx1/delta_0000001_0000001_0000
# drwxr-xr-x   - itv020752 supergroup          0 2025-08-14 09:14 warehouse/misgaurav_acid.db/orders_trx1/delta_0000002_0000002_0000; what has to be inserted

# hive> select * from orders_trx1;
# OK
# 1       2013-07-25 00:00:00.0   45      CLOSED
# Time taken: 33.187 seconds, Fetched: 1 row(s)

# this will create again 1 more directory

# hive> insert into orders_trx1 values (2,"2013-07-25 00:00:00.0",47,"CLOSED");
# Query ID = itv020752_20250814092041_8888470a-45bc-483c-b385-3464f3a83b9a
# Total jobs = 1
# Launching Job 1 out of 1
# Number of reduce tasks determined at compile time: 1
# In order to change the average load for a reducer (in bytes):
#   set hive.exec.reducers.bytes.per.reducer=<number>
# In order to limit the maximum number of reducers:
#   set hive.exec.reducers.max=<number>
# In order to set a constant number of reducers:
#   set mapreduce.job.reduces=<number>
# Starting Job = job_1754561546788_3393, Tracking URL = http://m02.itversity.com:19088/proxy/application_1754561546788_3393/
# Kill Command = /opt/hadoop/bin/mapred job  -kill job_1754561546788_3393
# Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
# 2025-08-14 09:21:20,624 Stage-1 map = 0%,  reduce = 0%
# 2025-08-14 09:21:25,742 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 2.44 sec
# 2025-08-14 09:21:29,883 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 3.95 sec
# MapReduce Total cumulative CPU time: 3 seconds 950 msec
# Ended Job = job_1754561546788_3393
# Loading data to table misgaurav_acid.orders_trx1
# MapReduce Jobs Launched: 
# Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 3.95 sec   HDFS Read: 18686 HDFS Write: 1252 SUCCESS
# Total MapReduce CPU Time Spent: 3 seconds 950 msec
# OK
# Time taken: 52.798 seconds


# [itv020752@g01 ~]$ hadoop fs -ls warehouse/misgaurav_acid.db/orders_trx1/
# Found 4 items
# drwxr-xr-x   - itv020752 supergroup          0 2025-08-14 09:14 warehouse/misgaurav_acid.db/orders_trx1/delete_delta_0000002_0000002_0000
# drwxr-xr-x   - itv020752 supergroup          0 2025-08-14 08:47 warehouse/misgaurav_acid.db/orders_trx1/delta_0000001_0000001_0000
# drwxr-xr-x   - itv020752 supergroup          0 2025-08-14 09:14 warehouse/misgaurav_acid.db/orders_trx1/delta_0000002_0000002_0000
# drwxr-xr-x   - itv020752 supergroup          0 2025-08-14 09:21 warehouse/misgaurav_acid.db/orders_trx1/delta_0000003_0000003_0000

# this will create again 1 more directory


# hive> delete from orders_trx1 where order_id = 2;
# Query ID = itv020752_20250814092258_75a2342b-f098-4774-923a-94a01fcb0e66
# Total jobs = 1
# Launching Job 1 out of 1
# Number of reduce tasks not specified. Estimated from input data size: 1
# In order to change the average load for a reducer (in bytes):
#   set hive.exec.reducers.bytes.per.reducer=<number>
# In order to limit the maximum number of reducers:
#   set hive.exec.reducers.max=<number>
# In order to set a constant number of reducers:
#   set mapreduce.job.reduces=<number>
# Starting Job = job_1754561546788_3396, Tracking URL = http://m02.itversity.com:19088/proxy/application_1754561546788_3396/
# Kill Command = /opt/hadoop/bin/mapred job  -kill job_1754561546788_3396
# Hadoop job information for Stage-1: number of mappers: 3; number of reducers: 1
# 2025-08-14 09:23:08,860 Stage-1 map = 0%,  reduce = 0%
# 2025-08-14 09:23:14,001 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 10.54 sec
# 2025-08-14 09:23:19,154 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 12.57 sec
# MapReduce Total cumulative CPU time: 12 seconds 570 msec
# Ended Job = job_1754561546788_3396
# Loading data to table misgaurav_acid.orders_trx1
# MapReduce Jobs Launched: 
# Stage-Stage-1: Map: 3  Reduce: 1   Cumulative CPU: 12.57 sec   HDFS Read: 32476 HDFS Write: 862 SUCCESS
# Total MapReduce CPU Time Spent: 12 seconds 570 msec
# OK
# Time taken: 22.245 seconds



# [itv020752@g01 ~]$ hadoop fs -ls warehouse/misgaurav_acid.db/orders_trx1/
# Found 5 items
# drwxr-xr-x   - itv020752 supergroup          0 2025-08-14 09:14 warehouse/misgaurav_acid.db/orders_trx1/delete_delta_0000002_0000002_0000
# drwxr-xr-x   - itv020752 supergroup          0 2025-08-14 09:23 warehouse/misgaurav_acid.db/orders_trx1/delete_delta_0000004_0000004_0000
# drwxr-xr-x   - itv020752 supergroup          0 2025-08-14 08:47 warehouse/misgaurav_acid.db/orders_trx1/delta_0000001_0000001_0000
# drwxr-xr-x   - itv020752 supergroup          0 2025-08-14 09:14 warehouse/misgaurav_acid.db/orders_trx1/delta_0000002_0000002_0000
# drwxr-xr-x   - itv020752 supergroup          0 2025-08-14 09:21 warehouse/misgaurav_acid.db/orders_trx1/delta_0000003_0000003_0000

# it will take the consoldiated files/views, it knows everything, what has been deleted, what has been inserted, and what has been updated.
# it will returns you a consolidated view of the data.

# hive> select * from orders_trx1;
# OK
# 1       2013-07-25 00:00:00.0   45      CLOSED
# Time taken: 33.718 seconds, Fetched: 1 row(s)

# as long as we keep doing insert, update, and delete commands, it will create a s many folder, hence meta data will become overburden to the NN.
# i.e, internally, it's doing hive compacts acid transactions files automatically, it will run in non-peak hours.
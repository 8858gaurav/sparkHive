# To create an Insert only ACID table, me must additionally specify the
# transactional property as insert_only

# All file formats are supported with the insert-only transactional table

# hive> CREATE TABLE IF NOT EXISTS orders_trx2 (
#     > order_id integer,
#     > order_date string,
#     > customer_id integer,
#     > order_status string
#     > )
#     > ROW FORMAT DELIMITED
#     > FIELDS TERMINATED BY ','
#     > STORED AS TEXTFILE
#     > TBLPROPERTIES ('transactional'='true',
#     > 'transactional_properties'='insert_only');
# OK
# Time taken: 0.248 seconds

# hive> describe formatted orders_trx2;
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
# CreateTime:             Thu Aug 14 09:43:35 EDT 2025     
# LastAccessTime:         UNKNOWN                  
# Retention:              0                        
# Location:               hdfs://m01.itversity.com:9000/user/itv020752/warehouse/misgaurav_acid.db/orders_trx2     
# Table Type:             MANAGED_TABLE            
# Table Parameters:                
#         COLUMN_STATS_ACCURATE   {\"BASIC_STATS\":\"true\",\"COLUMN_STATS\":{\"customer_id\":\"true\",\"order_date\":\"true\",\"order_id\":\
# "true\",\"order_status\":\"true\"}}
#         bucketing_version       2                   
#         numFiles                0                   
#         numRows                 0                   
#         rawDataSize             0                   
#         totalSize               0                   
#         transactional           true                
#         transactional_properties        insert_only         
#         transient_lastDdlTime   1755179015          
                 
# # Storage Information            
# SerDe Library:          org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe       
# InputFormat:            org.apache.hadoop.mapred.TextInputFormat         
# OutputFormat:           org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat       
# Compressed:             No                       
# Num Buckets:            -1                       
# Bucket Columns:         []                       
# Sort Columns:           []                       
# Storage Desc Params:             
#         field.delim             ,                   
#         serialization.format    ,                   
# Time taken: 0.189 seconds, Fetched: 37 row(s)

# hive> insert into orders_trx2 values (1,"2013-07-25 00:00:00.0",45,"COMPLETE");
# Query ID = itv020752_20250814094528_8acf7c0e-4fc2-4531-98dd-e4c17e74a296
# Total jobs = 3
# Launching Job 1 out of 3
# Number of reduce tasks determined at compile time: 1
# In order to change the average load for a reducer (in bytes):
#   set hive.exec.reducers.bytes.per.reducer=<number>
# In order to limit the maximum number of reducers:
#   set hive.exec.reducers.max=<number>
# In order to set a constant number of reducers:
#   set mapreduce.job.reduces=<number>
# Starting Job = job_1754561546788_3418, Tracking URL = http://m02.itversity.com:19088/proxy/application_1754561546788_3418/
# Kill Command = /opt/hadoop/bin/mapred job  -kill job_1754561546788_3418
# Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
# 2025-08-14 09:46:13,177 Stage-1 map = 0%,  reduce = 0%
# 2025-08-14 09:46:18,358 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 2.43 sec
# 2025-08-14 09:46:22,515 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 4.15 sec
# MapReduce Total cumulative CPU time: 4 seconds 150 msec
# Ended Job = job_1754561546788_3418
# Stage-4 is selected by condition resolver.
# Stage-3 is filtered out by condition resolver.
# Stage-5 is filtered out by condition resolver.
# Loading data to table misgaurav_acid.orders_trx2
# MapReduce Jobs Launched: 
# Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 4.15 sec   HDFS Read: 18243 HDFS Write: 476 SUCCESS
# Total MapReduce CPU Time Spent: 4 seconds 150 msec
# OK
# Time taken: 57.033 seconds

# [itv020752@g01 ~]$ hadoop fs -ls warehouse/misgaurav_acid.db/orders_trx2/
# Found 1 items
# drwxr-xr-x   - itv020752 supergroup          0 2025-08-14 09:46 warehouse/misgaurav_acid.db/orders_trx2/delta_0000001_0000001_0000

# Found 1 items
# -rw-r--r--   3 itv020752 supergroup         36 2025-08-14 09:46 warehouse/misgaurav_acid.db/orders_trx2/delta_0000001_0000001_0000/000000_0

# update, & delete commands will not work here.

# hive > update orders_trx2 SET order_status = "CLOSED" where order_id=1;
# FAILED: SemanticException [Error 10414]: Attempt to do update or delete on table misgaurav_acid.orders_trx2 that is insert-only transactional

# hive> delete from orders_trx2 where order_id=1;
# FAILED: SemanticException [Error 10414]: Attempt to do update or delete on table misgaurav_acid.orders_trx2 that is insert-only transactional

# can we convert a non ACID table to a ACID - is possible
# can we convert a ACID table to a non ACID one - it is not possible
# we can only convert a non-ACID managed table into a ACID table.
# we can't convert non-acid external table into a ACID table.

# if you have to convert an External hive table into hive ACID table,
# you must first convert it to a non-ACID managed table by running below
# command

# hive> CREATE EXTERNAL TABLE IF NOT EXISTS orders_external1 (
#     > order_id integer,
#     > order_date string,
#     > customer_id integer,
#     > order_status string
#     > )
#     > ROW FORMAT DELIMITED
#     > FIELDS TERMINATED BY ','
#     > STORED AS TEXTFILE
#     > LOCATION '/user/itv020752/hive_datasets/orders1';
# OK
# Time taken: 0.268 seconds
# hive> 

# this is a non-transactional ext tables.

# hive> select * from orders_external1;
# OK
# 5       2013-07-25 00:00:00.0   11318   COMPLETESS
# 6       2013-07-25 00:00:00.0   7130    COMPLETESS
# Time taken: 33.662 seconds, Fetched: 2 row(s)

# describe formatted orders_external1

# hive> describe formatted orders_external1;
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
# CreateTime:             Thu Aug 14 10:02:16 EDT 2025     
# LastAccessTime:         UNKNOWN                  
# Retention:              0                        
# Location:               hdfs://m01.itversity.com:9000/user/itv020752/hive_datasets/orders1       
# Table Type:             EXTERNAL_TABLE           
# Table Parameters:                
#         EXTERNAL                TRUE                
#         bucketing_version       2                   
#         numFiles                1                   
#         totalSize               81                  
#         transient_lastDdlTime   1755180136          
                 
# # Storage Information            
# SerDe Library:          org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe       
# InputFormat:            org.apache.hadoop.mapred.TextInputFormat         
# OutputFormat:           org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat       
# Compressed:             No                       
# Num Buckets:            -1                       
# Bucket Columns:         []                       
# Sort Columns:           []                       
# Storage Desc Params:             
#         field.delim             ,                   
#         serialization.format    ,                   
# Time taken: 0.104 seconds, Fetched: 33 row(s)

# ALTER TABLE orders_external1 set tblproperties('EXTERNAL'='FALSE'); from ext table we are converting it to a managed tables.

# hive> ALTER TABLE orders_external1 set tblproperties('EXTERNAL'='FALSE');
# OK
# Time taken: 0.34 seconds

# now its a managed table

# hive> describe formatted orders_external1;
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
# CreateTime:             Thu Aug 14 10:02:16 EDT 2025     
# LastAccessTime:         UNKNOWN                  
# Retention:              0                        
# Location:               hdfs://m01.itversity.com:9000/user/itv020752/hive_datasets/orders1       
# Table Type:             MANAGED_TABLE            
# Table Parameters:                
#         EXTERNAL                FALSE               
#         bucketing_version       2                   
#         last_modified_by        itv020752           
#         last_modified_time      1755180358          
#         numFiles                1                   
#         totalSize               81                  
#         transient_lastDdlTime   1755180358          
                 
# # Storage Information            
# SerDe Library:          org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe       
# InputFormat:            org.apache.hadoop.mapred.TextInputFormat         
# OutputFormat:           org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat       
# Compressed:             No                       
# Num Buckets:            -1                       
# Bucket Columns:         []                       
# Sort Columns:           []                       
# Storage Desc Params:             
#         field.delim             ,                   
#         serialization.format    ,                   
# Time taken: 0.188 seconds, Fetched: 35 row(s)

# A managed table can now be converted into a transaction ACID table..
# ALTER TABLE orders_external1 SET TBLPROPERTIES
# ('transactional'='true','transactional_properties'='insert_only')

# with text file in this case, we can go with insert only. with ORC file we can go full pledge with ACID transactional properties.
# you can convert a non-ACID hive table to a full ACID TABLE only when the non-ACID table data is in orc format


# hive> ALTER TABLE orders_external1 SET TBLPROPERTIES
#     > ('transactional'='true','transactional_properties'='insert_only');
# Moving data to directory hdfs://m01.itversity.com:9000/user/itv020752/hive_datasets/orders1/delta_0000001_0000001_0000
# OK
# Time taken: 1.598 seconds


# hive> describe formatted orders_external1;
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
# CreateTime:             Thu Aug 14 10:02:16 EDT 2025     
# LastAccessTime:         UNKNOWN                  
# Retention:              0                        
# Location:               hdfs://m01.itversity.com:9000/user/itv020752/hive_datasets/orders1       
# Table Type:             MANAGED_TABLE            
# Table Parameters:                
#         EXTERNAL                FALSE               
#         bucketing_version       2                   
#         last_modified_by        itv020752           
#         last_modified_time      1755180628          
#         numFiles                1                   
#         totalSize               81                  
#         transactional           true                
#         transactional_properties        insert_only         
#         transient_lastDdlTime   1755180628          
                 
# # Storage Information            
# SerDe Library:          org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe       
# InputFormat:            org.apache.hadoop.mapred.TextInputFormat         
# OutputFormat:           org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat       
# Compressed:             No                       
# Num Buckets:            -1                       
# Bucket Columns:         []                       
# Sort Columns:           []                       
# Storage Desc Params:             
#         field.delim             ,                   
#         serialization.format    ,                   
# Time taken: 0.225 seconds, Fetched: 37 row(s)
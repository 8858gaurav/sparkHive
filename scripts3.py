import pyspark, pandas as pd
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import getpass, time, os
username = getpass.getuser()
print(username)


if __name__ == '__main__':
    print("creating spark session")

    spark = SparkSession \
           .builder \
           .appName("schemaEvolutions") \
           .config("spark.shuffle.useOldFetchProtocol","true") \
           .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse") \
           .enableHiveSupport() \
           .master("yarn") \
           .getOrCreate()
    
    ###################################################

    # hive> create external table misgaurav_orders_ext (order_id string, order_date string, customer_id string, order_status string)
    #     > row format delimited
    #     > fields terminated by ','
    #     > stored as textfile;
    # FAILED: Execution Error, return code 1 from org.apache.hadoop.hive.ql.exec.DDLTask. MetaException(message:Got exception: org.apache.hadoop.
    # security.AccessControlException Permission denied: user=itv020752, access=WRITE, inode="/user":hdfs:supergroup:drwxr-xr-x

    # hive> set hive.metastore.warehouse.dir=/user/itv020752/warehouse;
    # hive> create external table misgaurav_orders_ext (order_id string, order_date string, customer_id string, order_status string)
    #     > row format delimited
    #     > fields terminated by ','
    #     > stored as textfile;
    # OK
    # Time taken: 0.741 seconds
    # hive> show tables;
    # OK
    # misgaurav_orders_ext
    # Time taken: 0.045 seconds, Fetched: 1 row(s)

    # [itv020752@g01 ~]$ hadoop fs -ls /user/itv020752/warehouse/misgaurav_101.db
    # Found 1 items
    # drwxr-xr-x   - itv020752 supergroup          0 2025-08-15 01:23 /user/itv020752/warehouse/misgaurav_101.db/misgaurav_orders_ext
    
    ############################################################################

    # itv020752@g01 ~]$ hadoop fs -ls /user/itv020752/data/datasets/hive_datasets
    # Found 1 items
    # -rw-r--r--   3 itv020752 supergroup         81 2025-08-14 06:10 /user/itv020752/data/datasets/hive_datasets/new_orders2

    # itv020752@g01 ~]$ hadoop fs -cat /user/itv020752/data/datasets/hive_datasets/new_orders2
    # 5,2013-07-25 00:00:00.0,11318,COMPLETESS
    # 6,2013-07-25 00:00:00.0,7130,COMPLETESS

    # hive> create external table misgaurav_orders_ext (order_id string, order_date string, customer_id string, order_status string)
    #     > row format delimited
    #     > fields terminated by ','
    #     > stored as textfile
    #     > location '/user/itv020752/data/datasets/hive_datasets';
    # OK
    # Time taken: 0.218 seconds
    # hive> 

    # hive> describe misgaurav_orders_ext;
    # OK
    # order_id                string                                      
    # order_date              string                                      
    # customer_id             string                                      
    # order_status            string                                      
    # Time taken: 0.07 seconds, Fetched: 4 row(s)

    # hive> describe extended misgaurav_orders_ext;
    # OK
    # order_id                string                                      
    # order_date              string                                      
    # customer_id             string                                      
    # order_status            string                                      
                    
    # Detailed Table Information      Table(tableName:misgaurav_orders_ext, dbName:metastore, owner:itv020752, createTime:1755166367, lastAccessT
    # ime:0, retention:0, sd:StorageDescriptor(cols:[FieldSchema(name:order_id, type:string, comment:null), FieldSchema(name:order_date, type:str
    # ing, comment:null), FieldSchema(name:customer_id, type:string, comment:null), FieldSchema(name:order_status, type:string, comment:null)], l
    # ocation:hdfs://m01.itversity.com:9000/user/itv020752/data/datasets/hive_datasets, inputFormat:org.apache.hadoop.mapred.TextInputFormat, out
    # putFormat:org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat, compressed:false, numBuckets:-1, serdeInfo:SerDeInfo(name:null, seria
    # lizationLib:org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, parameters:{serialization.format=,, field.delim=,}), bucketCols:[], sortCol
    # s:[], parameters:{}, skewedInfo:SkewedInfo(skewedColNames:[], skewedColValues:[], skewedColValueLocationMaps:{}), storedAsSubDirectories:fa
    # lse), partitionKeys:[], parameters:{transient_lastDdlTime=1755166367, bucketing_version=2, totalSize=81, EXTERNAL=TRUE, numFiles=1}, viewOr
    # iginalText:null, viewExpandedText:null, tableType:EXTERNAL_TABLE, rewriteEnabled:false, catName:hive, ownerType:USER)
    # Time taken: 0.061 seconds, Fetched: 6 row(s)

    # hive> select * from misgaurav_orders_ext;
    # OK
    # 5       2013-07-25 00:00:00.0   11318   COMPLETESS
    # 6       2013-07-25 00:00:00.0   7130    COMPLETESS
    # Time taken: 32.866 seconds, Fetched: 2 row(s)

    spark.sql("select * from misgaurav_orders_ext").show()
    # this will not wrok here for external hive table, since we don't have write access to the warehouse directory.

    # now placing a new file inside this folder /user/itv020752/data/datasets/hive_datasets

    # [itv020752@g01 ~]$ hadoop fs -put new_orders1 /user/itv020752/data/datasets/hive_datasets

    # hive> select * from misgaurav_orders_ext;
    # OK
    # 5       2013-07-25      11318   COMPLETE
    # 6       2013-07-25      7130    COMPLETES
    # 5       2013-07-25 00:00:00.0   11318   COMPLETESS
    # 6       2013-07-25 00:00:00.0   7130    COMPLETESS
    # Time taken: 30.23 seconds, Fetched: 4 row(s)

    # now loading a file locally to a table : misgaurav_orders_ext

    # hive> load data local inpath 'new_orders' into table misgaurav_orders_ext;
    # Loading data to table metastore.misgaurav_orders_ext
    # OK
    # Time taken: 0.375 seconds

    # hive> select * from misgaurav_orders_ext;
    # OK
    # 5       2013-07-25 00:00:00.0   11318   COMPLETE
    # 6       2013-07-25 00:00:00.0   7130    COMPLETES
    # 5       2013-07-25      11318   COMPLETE
    # 6       2013-07-25      7130    COMPLETES
    # 5       2013-07-25 00:00:00.0   11318   COMPLETESS
    # 6       2013-07-25 00:00:00.0   7130    COMPLETESS
    # Time taken: 31.501 seconds, Fetched: 6 row(s)

    # now loading a file from hdfs to a table : misgaurav_orders_ext
    # hadoop fs -put new_orders3 /user/itv020752/data/datasets/hive_datasets


    # this is your hdfs path: data/new_orders3
    # hive> load data inpath 'data/new_orders3' into table misgaurav_orders_ext;
    # Loading data to table metastore.misgaurav_orders_ext
    # OK
    # Time taken: 0.44 seconds

    # hive> select * from misgaurav_orders_ext;
    # OK
    # 5       2013-07-25 00:00:00.0   11318   COMPLETE
    # 6       2013-07-25 00:00:00.0   7130    COMPLETES
    # 5       2013-07-25      11318   COMPLETE
    # 6       2013-07-25      7130    COMPLETES
    # 5       2013-07-25 00:00:00.0   11318   COMPLETESS
    # 6       2013-07-25 00:00:00.0   7130    COMPLETESS
    # 5       2013-07-25 00:00:00.0   11318   COMPLET
    # 6       2013-07-25 00:00:00.0   7130    COMPLET
    # Time taken: 32.411 seconds, Fetched: 8 row(s)

    # itv020752@g01 ~]$ hadoop fs -ls /user/itv020752/data/datasets/hive_datasets
    # Found 4 items
    # -rw-r--r--   3 itv020752 supergroup         78 2025-08-14 06:21 /user/itv020752/data/datasets/hive_datasets/new_orders
    # -rw-r--r--   3 itv020752 supergroup         56 2025-08-14 06:19 /user/itv020752/data/datasets/hive_datasets/new_orders1
    # -rw-r--r--   3 itv020752 supergroup         81 2025-08-14 06:10 /user/itv020752/data/datasets/hive_datasets/new_orders2
    # -rw-r--r--   3 itv020752 supergroup         75 2025-08-14 06:28 /user/itv020752/data/datasets/hive_datasets/new_orders3

    # The data will be still there, only the table will be dropped.
    # hive> drop table misgaurav_orders_ext;
    # OK
    # Time taken: 0.279 seconds
    # hive> select * from misgaurav_orders_ext;
    # FAILED: SemanticException [Error 10001]: Line 1:14 Table not found 'misgaurav_orders_ext'

    # [itv020752@g01 ~]$ hadoop fs -ls /user/itv020752/data/datasets/hive_datasets
    # Found 4 items
    # -rw-r--r--   3 itv020752 supergroup         78 2025-08-14 06:21 /user/itv020752/data/datasets/hive_datasets/new_orders
    # -rw-r--r--   3 itv020752 supergroup         56 2025-08-14 06:19 /user/itv020752/data/datasets/hive_datasets/new_orders1
    # -rw-r--r--   3 itv020752 supergroup         81 2025-08-14 06:10 /user/itv020752/data/datasets/hive_datasets/new_orders2
    # -rw-r--r--   3 itv020752 supergroup         75 2025-08-14 06:28 /user/itv020752/data/datasets/hive_datasets/new_orders3
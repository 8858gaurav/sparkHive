# hive is not a db, it's a datawarehouse. data will be in hdfs, only the metastore of table will store under hive.
# for hdfs the same data is a flat file but for hive, it's a proper structured format.

# along with hive on each data node, we have to install metastore. 

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
           .appName("hiveTheory") \
           .config("spark.shuffle.useOldFetchProtocol","true") \
           .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse") \
           .enableHiveSupport() \
           .master("yarn") \
           .getOrCreate()
    
    spark.read.format("csv").schema('order_id long, order_date date, customer_id long, order_status string') \
    .load("/public/trendytech/datasets/parquet-schema-evol-demo/csv/orders3.csv") \
    .createOrReplaceTempView("orders")
    
    spark.sql("select * from orders").show()
    # +--------+----------+-----------+------------+
    # |order_id|order_date|customer_id|order_status|
    # +--------+----------+-----------+------------+
    # |       5|2013-07-25|      11318|    COMPLETE|
    # |       6|2013-07-25|       7130|    COMPLETE|
    # +--------+----------+-----------+------------+

    spark.sql("CREATE DATABASE misgaurav_hive").show()

    # hive> use misgaurav_hive;
    # OK

    # [itv020752@g01 ~]$ hadoop fs -ls /user/itv020752/warehouse
    # Found 3 items
    # drwxr-xr-x   - itv020752 supergroup          0 2025-07-17 10:05 /user/itv020752/warehouse/itv020752_db.db
    # drwxr-xr-x   - itv020752 supergroup          0 2025-07-17 10:15 /user/itv020752/warehouse/itv020752_partitioning.db
    # drwxr-xr-x   - itv020752 supergroup          0 2025-08-13 05:42 /user/itv020752/warehouse/misgaurav_hive.db

    spark.sql("CREATE TABLE IF NOT EXISTS misgaurav_hive.table1 (order_id integer, order_date date, customer_id integer, order_status string)").show()

    # hive> show tables;
    # OK
    # table1

    # [itv020752@g01 ~]$ hadoop fs -ls /user/itv020752/warehouse/misgaurav_hive.db
    # Found 1 items
    # drwxr-xr-x   - itv020752 supergroup          0 2025-08-13 05:46 /user/itv020752/warehouse/misgaurav_hive.db/table1

    spark.sql("insert into misgaurav_hive.table1 (select * from orders)")

    # [itv020752@g01 ~]$ hadoop fs -ls /user/itv020752/warehouse/misgaurav_hive.db/table1
    # Found 1 items
    # -rwxr-xr-x   3 itv020752 supergroup         55 2025-08-13 05:46 /user/itv020752/warehouse/misgaurav_hive.db/table1/part-00000-11bafd11-0851
    # -4fe6-b064-1741062b9c9a-c000


    # [itv020752@g01 ~]$ hadoop fs -cat /user/itv020752/warehouse/misgaurav_hive.db/table1/part-00000-11bafd11-0851-4fe6-b064-1741062b9c9a-c000
    # 52013-07-2511318COMPLETE
    # 62013-07-257130COMPLETE

    spark.sql("describe table misgaurav_hive.table1").show()
    # +------------+---------+-------+
    # |    col_name|data_type|comment|
    # +------------+---------+-------+
    # |    order_id|      int|   null|
    # |  order_date|     date|   null|
    # | customer_id|      int|   null|
    # |order_status|   string|   null|
    # +------------+---------+-------+

    spark.sql("describe extended misgaurav_hive.table1").show(truncate = False)
    # +----------------------------+-------------------------------------------------------------------------------+-------+
    # |col_name                    |data_type                                                                      |comment|
    # +----------------------------+-------------------------------------------------------------------------------+-------+
    # |order_id                    |int                                                                            |null   |
    # |order_date                  |date                                                                           |null   |
    # |customer_id                 |int                                                                            |null   |
    # |order_status                |string                                                                         |null   |
    # |                            |                                                                               |       |
    # |# Detailed Table Information|                                                                               |       |
    # |Database                    |misgaurav_hive                                                                 |       |
    # |Table                       |table1                                                                         |       |
    # |Owner                       |itv020752                                                                      |       |
    # |Created Time                |Wed Aug 13 05:42:09 EDT 2025                                                   |       |
    # |Last Access                 |UNKNOWN                                                                        |       |
    # |Created By                  |Spark 3.1.2                                                                    |       |
    # |Type                        |MANAGED                                                                        |       |
    # |Provider                    |hive                                                                           |       |
    # |Table Properties            |[transient_lastDdlTime=1755078403]                                             |       |
    # |Statistics                  |55 bytes                                                                       |       |
    # |Location                    |hdfs://m01.itversity.com:9000/user/itv020752/warehouse/misgaurav_hive.db/table1|       |
    # |Serde Library               |org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe                             |       |
    # |InputFormat                 |org.apache.hadoop.mapred.TextInputFormat                                       |       |
    # |OutputFormat                |org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat                     |       |
    # +----------------------------+-------------------------------------------------------------------------------+-------+

    spark.sql("describe formatted misgaurav_hive.table1").show(truncate = False)
    # +----------------------------+-------------------------------------------------------------------------------+-------+
    # |col_name                    |data_type                                                                      |comment|
    # +----------------------------+-------------------------------------------------------------------------------+-------+
    # |order_id                    |int                                                                            |null   |
    # |order_date                  |date                                                                           |null   |
    # |customer_id                 |int                                                                            |null   |
    # |order_status                |string                                                                         |null   |
    # |                            |                                                                               |       |
    # |# Detailed Table Information|                                                                               |       |
    # |Database                    |misgaurav_hive                                                                 |       |
    # |Table                       |table1                                                                         |       |
    # |Owner                       |itv020752                                                                      |       |
    # |Created Time                |Wed Aug 13 05:42:09 EDT 2025                                                   |       |
    # |Last Access                 |UNKNOWN                                                                        |       |
    # |Created By                  |Spark 3.1.2                                                                    |       |
    # |Type                        |MANAGED                                                                        |       |
    # |Provider                    |hive                                                                           |       |
    # |Table Properties            |[transient_lastDdlTime=1755078403]                                             |       |
    # |Statistics                  |55 bytes                                                                       |       |
    # |Location                    |hdfs://m01.itversity.com:9000/user/itv020752/warehouse/misgaurav_hive.db/table1|       |
    # |Serde Library               |org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe                             |       |
    # |InputFormat                 |org.apache.hadoop.mapred.TextInputFormat                                       |       |
    # |OutputFormat                |org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat                     |       |
    # +----------------------------+-------------------------------------------------------------------------------+-------+

    spark.sql("select * from misgaurav_hive.table1").show()
    # +--------+----------+-----------+------------+
    # |order_id|order_date|customer_id|order_status|
    # +--------+----------+-----------+------------+
    # |       5|2013-07-25|      11318|    COMPLETE|
    # |       6|2013-07-25|       7130|    COMPLETE|
    # +--------+----------+-----------+------------+

    # now adding the file in this path
    # [itv020752@g01 ~]$ hadoop fs -ls /user/itv020752/warehouse/misgaurav_hive.db/table1

    # [itv020752@g01 ~]$ hadoop fs -put new_orders /user/itv020752/warehouse/misgaurav_hive.db/table1/

    spark.sql("select * from misgaurav_hive.table1").show()

    # +--------+----------+-----------+------------+
    # |order_id|order_date|customer_id|order_status|
    # +--------+----------+-----------+------------+
    # |    null|      null|       null|        null|
    # |    null|      null|       null|        null|
    # |       5|2013-07-25|      11318|    COMPLETE|
    # |       6|2013-07-25|       7130|    COMPLETE|
    # +--------+----------+-----------+------------+


    # hive> select * from table1;
    # OK
    # NULL    NULL    NULL    NULL
    # NULL    NULL    NULL    NULL
    # 5       2013-07-25      11318   COMPLETE
    # 6       2013-07-25      7130    COMPLETE
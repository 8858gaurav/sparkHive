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

    spark.sql("CREATE DATABASE misgaurav_hive_new").show()

    # hive> use misgaurav_hive_new;
    # OK

    # [itv020752@g01 ~]$ hadoop fs -ls /user/itv020752/warehouse
    # Found 3 items
    # drwxr-xr-x   - itv020752 supergroup          0 2025-07-17 10:05 /user/itv020752/warehouse/itv020752_db.db
    # drwxr-xr-x   - itv020752 supergroup          0 2025-07-17 10:15 /user/itv020752/warehouse/itv020752_partitioning.db
    # drwxr-xr-x   - itv020752 supergroup          0 2025-08-13 05:42 /user/itv020752/warehouse/misgaurav_hive_new.db

    spark.sql("CREATE TABLE IF NOT EXISTS misgaurav_hive_new.table1 (order_id string, order_date string, customer_id string, order_status string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ").show()

    # hive> show tables;
    # OK
    # table1

    # [itv020752@g01 ~]$ hadoop fs -ls /user/itv020752/warehouse/misgaurav_hive_new.db
    # Found 1 items
    # drwxr-xr-x   - itv020752 supergroup          0 2025-08-13 05:46 /user/itv020752/warehouse/misgaurav_hive_new.db/table1

    spark.sql("insert into misgaurav_hive_new.table1 (select * from orders)")

    # [itv020752@g01 ~]$ hadoop fs -ls /user/itv020752/warehouse/misgaurav_hive_new.db/table1
    # Found 1 items
    # -rwxr-xr-x   3 itv020752 supergroup         55 2025-08-13 05:46 /user/itv020752/warehouse/misgaurav_hive_new.db/table1/part-00000-fa864e12-e1b5-48f0-8f8b-d530c7f1fbf7-c000

    # [itv020752@g01 ~]$ hadoop fs -cat /user/itv020752/warehouse/misgaurav_hive_new.db/table1/part-00000-fa864e12-e1b5-48f0-8f8b-d530c7f1fbf7-c000
    # 5,2013-07-25,11318,COMPLETE
    # 6,2013-07-25,7130,COMPLETE

    spark.sql("describe table misgaurav_hive_new.table1").show()
    # +------------+---------+-------+
    # |    col_name|data_type|comment|
    # +------------+---------+-------+
    # |    order_id|   string|   null|
    # |  order_date|   string|   null|
    # | customer_id|   string|   null|
    # |order_status|   string|   null|
    # +------------+---------+-------+

    spark.sql("describe extended misgaurav_hive_new.table1").show(truncate = False)
    # +----------------------------+-------------------------------------------------------------------------------+-------+
    # |col_name                    |data_type                                                                      |comment|
    # +----------------------------+-------------------------------------------------------------------------------+-------+
    # |order_id                    |string                                                                         |null   |
    # |order_date                  |string                                                                         |null   |
    # |customer_id                 |string                                                                         |null   |
    # |order_status                |string                                                                         |null   |
    # |                            |                                                                               |       |
    # |# Detailed Table Information|                                                                               |       |
    # |Database                    |misgaurav_hive_new                                                             |       |
    # |Table                       |table1                                                                         |       |
    # |Owner                       |itv020752                                                                      |       |
    # |Created Time                |Wed Aug 13 06:47:29 EDT 2025                                                   |       |
    # |Last Access                 |UNKNOWN                                                                        |       |
    # |Created By                  |Spark 3.1.2                                                                    |       |
    # |Type                        |MANAGED                                                                        |       |
    # |Provider                    |hive                                                                           |       |
    # |Table Properties            |[transient_lastDdlTime=1755082054]                                             |       |
    # |Statistics                  |77 bytes                                                                       |       |
    # |Location                    |hdfs://m01.itversity.com:9000/user/itv020752/warehouse/misgaurav_hive_new.db/table1|       |
    # |Serde Library               |org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe                             |       |
    # |InputFormat                 |org.apache.hadoop.mapred.TextInputFormat                                       |       |
    # |OutputFormat                |org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat                     |       |
    # +----------------------------+-------------------------------------------------------------------------------+-------+

    spark.sql("describe formatted misgaurav_hive_new.table1").show(truncate = False)
    # +----------------------------+-------------------------------------------------------------------------------+-------+
    # |col_name                    |data_type                                                                      |comment|
    # +----------------------------+-------------------------------------------------------------------------------+-------+
    # |order_id                    |string                                                                         |null   |
    # |order_date                  |string                                                                         |null   |
    # |customer_id                 |string                                                                         |null   |
    # |order_status                |string                                                                         |null   |
    # |                            |                                                                               |       |
    # |# Detailed Table Information|                                                                               |       |
    # |Database                    |misgaurav_hive_new                                                             |       |
    # |Table                       |table1                                                                         |       |
    # |Owner                       |itv020752                                                                      |       |
    # |Created Time                |Wed Aug 13 06:47:29 EDT 2025                                                   |       |
    # |Last Access                 |UNKNOWN                                                                        |       |
    # |Created By                  |Spark 3.1.2                                                                    |       |
    # |Type                        |MANAGED                                                                        |       |
    # |Provider                    |hive                                                                           |       |
    # |Table Properties            |[transient_lastDdlTime=1755082054]                                             |       |
    # |Statistics                  |77 bytes                                                                       |       |
    # |Location                    |hdfs://m01.itversity.com:9000/user/itv020752/warehouse/misgaurav_hive_new.db/table1|       |
    # |Serde Library               |org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe                             |       |
    # |InputFormat                 |org.apache.hadoop.mapred.TextInputFormat                                       |       |
    # |OutputFormat                |org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat                     |       |
    # +----------------------------+-------------------------------------------------------------------------------+-------+

    spark.sql("select * from misgaurav_hive_new.table1").show()
    # +--------+----------+-----------+------------+
    # |order_id|order_date|customer_id|order_status|
    # +--------+----------+-----------+------------+
    # |       5|2013-07-25|      11318|    COMPLETE|
    # |       6|2013-07-25|       7130|    COMPLETE|
    # +--------+----------+-----------+------------+

    # now adding the file in this path
    # [itv020752@g01 ~]$ hadoop fs -ls /user/itv020752/warehouse/misgaurav_hive_new.db/table1

    # itv020752@g01 ~]$ hadoop fs -put new_orders1 /user/itv020752/warehouse/misgaurav_hive_new.db/table1/
        
    # [itv020752@g01 ~]$ hadoop fs -ls /user/itv020752/warehouse/misgaurav_hive_new.db/table1/
    # Found 2 items
    # -rw-r--r--   3 itv020752 supergroup         56 2025-08-13 07:22 /user/itv020752/warehouse/misgaurav_hive_new.db/table1/new_orders1
    # -rwxr-xr-x   3 itv020752 supergroup         55 2025-08-13 07:17 /user/itv020752/warehouse/misgaurav_hive_new.db/table1/part-00000-fa864e12-e1b5-48f0-8f8b-d530c7f1fbf7-c000
    
    spark.sql("select * from misgaurav_hive_new.table1").show()

    # +--------+----------+-----------+------------+
    # |order_id|order_date|customer_id|order_status|
    # +--------+----------+-----------+------------+
    # |       5|2013-07-25|      11318|    COMPLETE|
    # |       6|2013-07-25|       7130|   COMPLETES|
    # |       5|2013-07-25|      11318|    COMPLETE|
    # |       6|2013-07-25|       7130|    COMPLETE|
    # +--------+----------+-----------+------------+

    # hive> select * from table1;
    # OK
    # 5       2013-07-25      11318   COMPLETE
    # 6       2013-07-25      7130    COMPLETES
    # 5       2013-07-25      11318   COMPLETE
    # 6       2013-07-25      7130    COMPLETE
    # Time taken: 30.732 seconds, Fetched: 4 row(s)

    spark.sql("select * from misgaurav_hive_new.table1").show(truncate = False)
    # +--------+----------+-----------+------------+
    # |order_id|order_date|customer_id|order_status|
    # +--------+----------+-----------+------------+
    # |       5|2013-07-25|      11318|    COMPLETE|
    # |       6|2013-07-25|       7130|   COMPLETES|
    # |       5|2013-07-25|      11318|    COMPLETE|
    # |       6|2013-07-25|       7130|    COMPLETE|
    # +--------+----------+-----------+------------+


      # cd /opt/hive/conf # in labs, then open hive-site.xml files

       # <property>
       #     <name>hive.metastore.warehouse.dir</name>
       #     <value>/user/hive/warehouse</value>
       #     <description>location of default database for the warehouse</description>
       #   </property>

       # for meta store connection with Hive

       # <property>
       #     <name>hive.metastore.db.type</name>
       #     <value>postgres</value>
       #     <description>
       #       Expects one of [derby, oracle, mysql, mssql, postgres].
       #       Type of database used by the metastore. Information schema &amp; JDBCStorageHandler depend on it.
       #     </description>
       #   </property>

       # [itv020752@g01 conf]$ grep -n -i "javax" hive-site.xml

       # <property>
       #     <name>javax.jdo.option.ConnectionUserName</name>
       #     <value>hive</value>
       #     <description>Username to use against metastore database</description>
       #   </property>

       # <property>
       #     <name>hive.conf.hidden.list</name>
       #     <value>javax.jdo.option.ConnectionPassword,hive.server2.keystore.password,fs.s3.awsAccessKeyId,fs.s3.awsSecretAccessKey,fs.s3n.awsAccessKeyId,fs.s3n.awsSecretAccessKey,fs.s3a.access.key,fs.s3a.secret.key,fs.s3a.proxy.password,dfs.adls.oauth2.credential,fs.adl.oauth2.credential</value>
       #     <description>Comma separated list of configuration options which should not be read by normal user like passwords</description>
       #   </property>

       # <property>
       #     <name>javax.jdo.option.ConnectionURL</name>
       #     <value>jdbc:postgresql://g01.itversity.com:5432/metastore</value>
       #     <description>
       #       JDBC connect string for a JDBC metastore.
       #       To use SSL to encrypt/authenticate the connection, provide database-specific SSL flag in the connection URL.
       #       For example, jdbc:postgresql://myhost/db?ssl=true for postgres database.
       #     </description>
       #   </property>

       # <property>
       #     <name>javax.jdo.option.ConnectionDriverName</name>
       #     <value>org.postgresql.Driver</value>
       #     <description>Driver class name for a JDBC metastore</description>
       #   </property>
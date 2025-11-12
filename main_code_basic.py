import pyspark, pandas as pd
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import getpass, time
username = getpass.getuser()
print(username)

# demo for managed spark table.

spark = SparkSession \
           .builder \
           .appName("basic application") \
           .config('spark.ui.port', '0') \
           .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse") \
           .enableHiveSupport() \
           .master("yarn") \
           .getOrCreate()
spark.sparkContext.setLogLevel('WARN')


#!hadoop fs -ls data/
# Found 6 items
# -rw-r--r--   3 itv020752 supergroup   63963136 2025-07-19 09:40 data/Lung_Cancer.csv
# -rw-r--r--   3 itv020752 supergroup  365001114 2025-07-17 08:46 data/bigLog.txt
# drwxr-xr-x   - itv020752 supergroup          0 2025-08-14 06:09 data/datasets
# -rw-r--r--   3 itv020752 supergroup       4333 2025-11-11 07:33 data/orders.csv
# -rw-r--r--   3 itv020752 supergroup  527433728 2025-07-19 14:04 data/question_tags.csv
# -rw-r--r--   3 itv020752 supergroup   60201900 2025-07-17 02:34 data/students.csv

spark.catalog.listDatabases()[:2]
# [Database(name='0000000000000_msdian', description='', locationUri='hdfs://m01.itversity.com:9000/user/itv009768/warehouse/0000000000000_msdian.db'),
#  Database(name='0000000000000_naveen_db', description='', locationUri='hdfs://m01.itversity.com:9000/user/itv009240/warehouse/0000000000000_naveen_db.db')]

import re
d = []
for i in spark.catalog.listDatabases():
    if re.search(r"itv020752", i.name):
        d.append(i)

print(d)
# [Database(name='itv020752_db', description='', locationUri='hdfs://m01.itversity.com:9000/user/itv020752/warehouse/itv020752_db.db'),
#  Database(name='itv020752_db_new', description='', locationUri='hdfs://m01.itversity.com:9000/user/itv020752/warehouse/itv020752_db_new.db'),
#  Database(name='itv020752_hivetable_savemethod', description='', locationUri='hdfs://m01.itversity.com:9000/user/itv020752/warehouse/itv020752_hivetable_savemethod.db'),
#  Database(name='itv020752_partitioning', description='', locationUri='hdfs://m01.itversity.com:9000/user/itv020752/warehouse/itv020752_partitioning.db')]

spark.sql("show databases").filter("namespace like '%itv020752%'").show()
# +--------------------+
# |           namespace|
# +--------------------+
# |        itv020752_db|
# |    itv020752_db_new|
# |itv020752_hivetab...|
# |itv020752_partiti...|
# +--------------------+


#!hadoop fs -cat data/orders.csv | head
# order_id,order_date,customer_id,order_status
# 1,2013-07-25 00:00:00.0,11599,CLOSED
# 2,2013-07-25 00:00:00.0,256,PENDING_PAYMENT
# 3,2013-07-25 00:00:00.0,12111,COMPLETE
# 4,2013-07-25 00:00:00.0,8827,CLOSED
# 5,2013-07-25 00:00:00.0,11318,COMPLETE
# 6,2013-07-25 00:00:00.0,7130,COMPLETE
# 7,2013-07-25 00:00:00.0,4530,COMPLETE
# 8,2013-07-25 00:00:00.0,2911,PROCESSING
# 9,2013-07-25 00:00:00.0,5657,PENDING_PAYMENT

df = spark.read.csv("data/orders.csv", header=True, inferSchema=True)
df.createOrReplaceTempView("orders")

spark.sql("""
CREATE TABLE itv020752_db.table_demo1 (
    order_id STRING, 
    order_date STRING, 
    customer_id STRING, 
    order_status STRING
)
USING CSV
""")

spark.sql("insert into itv020752_db.table_demo1 select * from orders")
spark.sql("describe extended itv020752_db.table_demo1").show(truncate = False)
# +----------------------------+----------------------------------------------------------------------------------+-------+
# |col_name                    |data_type                                                                         |comment|
# +----------------------------+----------------------------------------------------------------------------------+-------+
# |order_id                    |string                                                                            |null   |
# |order_date                  |string                                                                            |null   |
# |customer_id                 |string                                                                            |null   |
# |order_status                |string                                                                            |null   |
# |                            |                                                                                  |       |
# |# Detailed Table Information|                                                                                  |       |
# |Database                    |itv020752_db                                                                      |       |
# |Table                       |table_demo1                                                                       |       |
# |Owner                       |itv020752                                                                         |       |
# |Created Time                |Wed Nov 12 09:08:59 EST 2025                                                      |       |
# |Last Access                 |UNKNOWN                                                                           |       |
# |Created By                  |Spark 3.1.2                                                                       |       |
# |Type                        |MANAGED                                                                           |       |
# |Provider                    |CSV                                                                               |       |
# |Location                    |hdfs://m01.itversity.com:9000/user/itv020752/warehouse/itv020752_db.db/table_demo1|       |
# |Serde Library               |org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe                                |       |
# |InputFormat                 |org.apache.hadoop.mapred.SequenceFileInputFormat                                  |       |
# |OutputFormat                |org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat                         |       |
# +----------------------------+----------------------------------------------------------------------------------+-------+

spark.sql("""
CREATE TABLE itv020752_db.table_demo2 (
    order_id STRING, 
    order_date STRING, 
    customer_id STRING, 
    order_status STRING
)
USING CSV LOCATION 'data/orders.csv'
""")

spark.sql("describe extended itv020752_db.table_demo2").show(truncate = False)
# +----------------------------+--------------------------------------------------------------------------------------+-------+
# |col_name                    |data_type                                                                             |comment|
# +----------------------------+--------------------------------------------------------------------------------------+-------+
# |order_id                    |string                                                                                |null   |
# |order_date                  |string                                                                                |null   |
# |customer_id                 |string                                                                                |null   |
# |order_status                |string                                                                                |null   |
# |                            |                                                                                      |       |
# |# Detailed Table Information|                                                                                      |       |
# |Database                    |itv020752_db                                                                          |       |
# |Table                       |table_demo2                                                                           |       |
# |Owner                       |itv020752                                                                             |       |
# |Created Time                |Wed Nov 12 09:13:52 EST 2025                                                          |       |
# |Last Access                 |UNKNOWN                                                                               |       |
# |Created By                  |Spark 3.1.2                                                                           |       |
# |Type                        |EXTERNAL                                                                              |       |
# |Provider                    |CSV                                                                                   |       |
# |Location                    |hdfs://m01.itversity.com:9000/user/itv020752/warehouse/itv020752_db.db/data/orders.csv|       |
# |Serde Library               |org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe                                    |       |
# |InputFormat                 |org.apache.hadoop.mapred.SequenceFileInputFormat                                      |       |
# |OutputFormat                |org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat                             |       |
# +----------------------------+--------------------------------------------------------------------------------------+-------+



spark.sql("""
CREATE TABLE IF NOT EXISTS itv020752_db.table_demo3 (
    order_id STRING, 
    order_date STRING, 
    customer_id STRING, 
    order_status STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'data/orders.csv'
""")

spark.sql("describe extended itv020752_db.table_demo3").show(truncate = False)
# +----------------------------+--------------------------------------------------------------------------------------+-------+
# |col_name                    |data_type                                                                             |comment|
# +----------------------------+--------------------------------------------------------------------------------------+-------+
# |order_id                    |string                                                                                |null   |
# |order_date                  |string                                                                                |null   |
# |customer_id                 |string                                                                                |null   |
# |order_status                |string                                                                                |null   |
# |                            |                                                                                      |       |
# |# Detailed Table Information|                                                                                      |       |
# |Database                    |itv020752_db                                                                          |       |
# |Table                       |table_demo3                                                                           |       |
# |Owner                       |itv020752                                                                             |       |
# |Created Time                |Wed Nov 12 09:17:03 EST 2025                                                          |       |
# |Last Access                 |UNKNOWN                                                                               |       |
# |Created By                  |Spark 3.1.2                                                                           |       |
# |Type                        |EXTERNAL                                                                              |       |
# |Provider                    |hive                                                                                  |       |
# |Table Properties            |[transient_lastDdlTime=1762957023]                                                    |       |
# |Location                    |hdfs://m01.itversity.com:9000/user/itv020752/warehouse/itv020752_db.db/data/orders.csv|       |
# |Serde Library               |org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe                                    |       |
# |InputFormat                 |org.apache.hadoop.mapred.TextInputFormat                                              |       |
# |OutputFormat                |org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat                            |       |
# |Storage Properties          |[serialization.format=,, field.delim=,]                                               |       |
# +----------------------------+--------------------------------------------------------------------------------------+-------+

spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS itv020752_db.table_demo4 (
    order_id STRING, 
    order_date STRING, 
    customer_id STRING, 
    order_status STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'data/orders.csv'
""")

spark.sql("describe extended itv020752_db.table_demo4").show(truncate = False)
# +----------------------------+--------------------------------------------------------------------------------------+-------+
# |col_name                    |data_type                                                                             |comment|
# +----------------------------+--------------------------------------------------------------------------------------+-------+
# |order_id                    |string                                                                                |null   |
# |order_date                  |string                                                                                |null   |
# |customer_id                 |string                                                                                |null   |
# |order_status                |string                                                                                |null   |
# |                            |                                                                                      |       |
# |# Detailed Table Information|                                                                                      |       |
# |Database                    |itv020752_db                                                                          |       |
# |Table                       |table_demo4                                                                           |       |
# |Owner                       |itv020752                                                                             |       |
# |Created Time                |Wed Nov 12 09:20:46 EST 2025                                                          |       |
# |Last Access                 |UNKNOWN                                                                               |       |
# |Created By                  |Spark 3.1.2                                                                           |       |
# |Type                        |EXTERNAL                                                                              |       |
# |Provider                    |hive                                                                                  |       |
# |Table Properties            |[transient_lastDdlTime=1762957246]                                                    |       |
# |Location                    |hdfs://m01.itversity.com:9000/user/itv020752/warehouse/itv020752_db.db/data/orders.csv|       |
# |Serde Library               |org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe                                    |       |
# |InputFormat                 |org.apache.hadoop.mapred.TextInputFormat                                              |       |
# |OutputFormat                |org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat                            |       |
# |Storage Properties          |[serialization.format=,, field.delim=,]                                               |       |
# +----------------------------+--------------------------------------------------------------------------------------+-------+

spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS itv020752_db.table_demo5 (
    order_id STRING, 
    order_date STRING, 
    customer_id STRING, 
    order_status STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
""")

#AnalysisException: CREATE EXTERNAL TABLE must be accompanied by LOCATION

spark.catalog.listTables()
# [Table(name='1htab', database='default', description=None, tableType='MANAGED', isTemporary=False),
#  Table(name='41group_movies', database='default', description=None, tableType='EXTERNAL', isTemporary=False)]

import re
e = []
for i in spark.catalog.listTables("itv020752_db"):
    if re.search(r"table_demo", i.name):
        e.append(i)

print(e)
# [Table(name='table_demo1', database='itv020752_db', description=None, tableType='MANAGED', isTemporary=False),
#  Table(name='table_demo2', database='itv020752_db', description=None, tableType='EXTERNAL', isTemporary=False),
#  Table(name='table_demo3', database='itv020752_db', description=None, tableType='EXTERNAL', isTemporary=False),
#  Table(name='table_demo4', database='itv020752_db', description=None, tableType='EXTERNAL', isTemporary=False)]

# !hadoop fs -ls /user/itv020752/warehouse/itv020752_db.db
# Found 3 items
# drwxr-xr-x   - itv020752 supergroup          0 2025-07-17 10:05 /user/itv020752/warehouse/itv020752_db.db/customer_bucketing
# drwxr-xr-x   - itv020752 supergroup          0 2025-11-12 09:17 /user/itv020752/warehouse/itv020752_db.db/data
# drwxr-xr-x   - itv020752 supergroup          0 2025-11-12 09:10 /user/itv020752/warehouse/itv020752_db.db/table_demo1
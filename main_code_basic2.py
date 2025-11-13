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

df = spark.read.csv("data/data1/orders.csv", header=True, inferSchema=True)

spark.sql("show databases").filter("namespace like '%itv020752_db%'").show()
# +----------------+
# |       namespace|
# +----------------+
# |    itv020752_db|
# |itv020752_db_new|
# +----------------+

spark.sql("USE itv020752_db")

print(spark.catalog.currentDatabase())
# itv020752_db

# Managed table, see https://github.com/8858gaurav/sparkHive/blob/main/main_code_basic.py
spark.sql("""
CREATE TABLE itv020752_db.table_demoa (
    order_id STRING, 
    order_date STRING, 
    customer_id STRING, 
    order_status STRING
)
USING CSV
""")
 
# External table, see https://github.com/8858gaurav/sparkHive/blob/main/main_code_basic.py
# always give this path: /user/itv020752/data/data1, not relative path: data/data1
spark.sql("""
CREATE TABLE itv020752_db.table_demob (
    order_id STRING, 
    order_date STRING, 
    customer_id STRING, 
    order_status STRING
)
USING CSV 
OPTIONS
(path '/user/itv020752/data/data1',
header 'true')
""")

df.createOrReplaceTempView("orders")
spark.sql("insert into itv020752_db.table_demoa select * from orders")

import re
f = []
for i in spark.catalog.listTables("itv020752_db"):
    if re.search(r'table_demo', i.name):
        f.append(i)
print(f)
# [Table(name='table_demo1', database='itv020752_db', description=None, tableType='MANAGED', isTemporary=False),
#  Table(name='table_demo2', database='itv020752_db', description=None, tableType='EXTERNAL', isTemporary=False),
#  Table(name='table_demo3', database='itv020752_db', description=None, tableType='EXTERNAL', isTemporary=False),
#  Table(name='table_demo4', database='itv020752_db', description=None, tableType='EXTERNAL', isTemporary=False),
#  Table(name='table_demoa', database='itv020752_db', description=None, tableType='MANAGED', isTemporary=False),
#  Table(name='table_demob', database='itv020752_db', description=None, tableType='EXTERNAL', isTemporary=False)]

spark.sql("describe extended table_demoa").show(truncate = False)
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
# |Table                       |table_demoa                                                                       |       |
# |Owner                       |itv020752                                                                         |       |
# |Created Time                |Thu Nov 13 07:02:23 EST 2025                                                      |       |
# |Last Access                 |UNKNOWN                                                                           |       |
# |Created By                  |Spark 3.1.2                                                                       |       |
# |Type                        |MANAGED                                                                           |       |
# |Provider                    |CSV                                                                               |       |
# |Location                    |hdfs://m01.itversity.com:9000/user/itv020752/warehouse/itv020752_db.db/table_demoa|       |
# |Serde Library               |org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe                                |       |
# |InputFormat                 |org.apache.hadoop.mapred.SequenceFileInputFormat                                  |       |
# |OutputFormat                |org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat                         |       |
# +----------------------------+----------------------------------------------------------------------------------+-------+

spark.sql("describe extended table_demob").show(truncate = False)
# +----------------------------+---------------------------------------------------------------------------------+-------+
# |col_name                    |data_type                                                                        |comment|
# +----------------------------+---------------------------------------------------------------------------------+-------+
# |order_id                    |string                                                                           |null   |
# |order_date                  |string                                                                           |null   |
# |customer_id                 |string                                                                           |null   |
# |order_status                |string                                                                           |null   |
# |                            |                                                                                 |       |
# |# Detailed Table Information|                                                                                 |       |
# |Database                    |itv020752_db                                                                     |       |
# |Table                       |table_demob                                                                      |       |
# |Owner                       |itv020752                                                                        |       |
# |Created Time                |Thu Nov 13 07:02:53 EST 2025                                                     |       |
# |Last Access                 |UNKNOWN                                                                          |       |
# |Created By                  |Spark 3.1.2                                                                      |       |
# |Type                        |EXTERNAL                                                                         |       |
# |Provider                    |CSV                                                                              |       |
# |Location                    |hdfs://m01.itversity.com:9000/user/itv020752/warehouse/itv020752_db.db/data/data1|       |
# |Serde Library               |org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe                               |       |
# |InputFormat                 |org.apache.hadoop.mapred.SequenceFileInputFormat                                 |       |
# |OutputFormat                |org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat                        |       |
# +----------------------------+---------------------------------------------------------------------------------+-------+

#!hadoop fs -ls /user/itv020752/warehouse/itv020752_db.db/
# Found 4 items
# drwxr-xr-x   - itv020752 supergroup          0 2025-07-17 10:05 /user/itv020752/warehouse/itv020752_db.db/customer_bucketing
# drwxr-xr-x   - itv020752 supergroup          0 2025-11-13 07:19 /user/itv020752/warehouse/itv020752_db.db/data
# drwxr-xr-x   - itv020752 supergroup          0 2025-11-12 11:39 /user/itv020752/warehouse/itv020752_db.db/table_demo1
# drwxr-xr-x   - itv020752 supergroup          0 2025-11-13 07:36 /user/itv020752/warehouse/itv020752_db.db/table_demoa

#!hadoop fs -ls /user/itv020752/warehouse/itv020752_db.db/data/
# Found 3 items
# drwxr-xr-x   - itv020752 supergroup          0 2025-11-13 07:19 /user/itv020752/warehouse/itv020752_db.db/data/data1
# drwxr-xr-x   - itv020752 supergroup          0 2025-11-12 12:02 /user/itv020752/warehouse/itv020752_db.db/data/orders.csv
# -rw-r--r--   3 itv020752 supergroup       4333 2025-11-12 11:50 /user/itv020752/warehouse/itv020752_db.db/data/orders_new.csv

#!hadoop fs -ls /user/itv020752/warehouse/itv020752_db.db/data/data1
# it's empty now

#!hadoop fs -ls /user/itv020752/warehouse/itv020752_db.db/table_demoa/
# Found 2 items
# -rw-r--r--   3 itv020752 supergroup          0 2025-11-13 07:36 /user/itv020752/warehouse/itv020752_db.db/table_demoa/_SUCCESS
# -rw-r--r--   3 itv020752 supergroup       4289 2025-11-13 07:36 /user/itv020752/warehouse/itv020752_db.db/table_demoa/part-00000-bb9dc5da-fcaf-454e-a13b-3976a3cdefd9-c000.csv

spark.sql("select count(*) from table_demoa").show()
# +--------+
# |count(1)|
# +--------+
# |     104|
# +--------+

spark.sql("select count(*) from table_demob").show()
# +--------+
# |count(1)|
# +--------+
# |     104|
# +--------+

#!hadoop fs -ls data/data1/
#Found 1 items
# -rw-r--r--   3 itv020752 supergroup       4333 2025-11-13 06:57 data/data1/orders.csv


# insert command will work for managed & ext tables both¶, before inserting, it's 104, & 104 in both ext, and managed tables.

spark.sql("""
INSERT INTO itv020752_db.table_demoa (order_id, order_date, customer_id, order_status)
VALUES ('1111','2013-07-25 00:00:00.0','256','PENDING_PAYMENT'),
       ('1112','2013-07-25 00:00:00.0','12111','COMPLETE')""")

spark.sql("""
INSERT INTO itv020752_db.table_demob (order_id, order_date, customer_id, order_status)
VALUES ('1111','2013-07-25 00:00:00.0','256','PENDING_PAYMENT'),
       ('1112','2013-07-25 00:00:00.0','12111','COMPLETE')""")

# after inserting by using insert commands

spark.sql("select count(*) from table_demoa").show()
# +--------+
# |count(1)|
# +--------+
# |     106|
# +--------+

spark.sql("select count(*) from table_demob").show()
# +--------+
# |count(1)|
# +--------+
# |     106|
# +--------+

# !hadoop fs -ls /user/itv020752/warehouse/itv020752_db.db/data/data1
# again, it's still empty now

#!hadoop fs -ls data/data1
# Found 4 items
# -rw-r--r--   3 itv020752 supergroup          0 2025-11-13 07:41 data/data1/_SUCCESS
# -rw-r--r--   3 itv020752 supergroup       4333 2025-11-13 06:57 data/data1/orders.csv
# -rw-r--r--   3 itv020752 supergroup         92 2025-11-13 07:41 data/data1/part-00000-c4534ce7-c68b-4d2b-9357-dedd65473e3a-c000.csv
# -rw-r--r--   3 itv020752 supergroup         87 2025-11-13 07:41 data/data1/part-00001-c4534ce7-c68b-4d2b-9357-dedd65473e3a-c000.csv

# external table create a file with column names, and inserted data.
# !hadoop fs -cat data/data1/part-00000-c4534ce7-c68b-4d2b-9357-dedd65473e3a-c000.csv
# order_id,order_date,customer_id,order_status
# 1111,2013-07-25 00:00:00.0,256,PENDING_PAYMENT

# external table create a file with column names, and inserted data.
# !hadoop fs -cat data/data1/part-00001-c4534ce7-c68b-4d2b-9357-dedd65473e3a-c000.csv
# order_id,order_date,customer_id,order_status
# 1112,2013-07-25 00:00:00.0,12111,COMPLETE

# !hadoop fs -ls /user/itv020752/warehouse/itv020752_db.db/table_demoa/
# Found 4 items
# -rw-r--r--   3 itv020752 supergroup          0 2025-11-13 07:41 /user/itv020752/warehouse/itv020752_db.db/table_demoa/_SUCCESS
# -rw-r--r--   3 itv020752 supergroup         47 2025-11-13 07:41 /user/itv020752/warehouse/itv020752_db.db/table_demoa/part-00000-180fa8f4-64de-47ce-8056-fda67457a592-c000.csv
# -rw-r--r--   3 itv020752 supergroup       4289 2025-11-13 07:36 /user/itv020752/warehouse/itv020752_db.db/table_demoa/part-00000-bb9dc5da-fcaf-454e-a13b-3976a3cdefd9-c000.csv
# -rw-r--r--   3 itv020752 supergroup         42 2025-11-13 07:41 /user/itv020752/warehouse/itv020752_db.db/table_demoa/part-00001-180fa8f4-64de-47ce-8056-fda67457a592-c000.csv

# managed table create a file without column names, but with inserted data.
# !hadoop fs -cat /user/itv020752/warehouse/itv020752_db.db/table_demoa/part-00000-180fa8f4-64de-47ce-8056-fda67457a592-c000.csv
# 1111,2013-07-25 00:00:00.0,256,PENDING_PAYMENT

# managed table create a file without column names, but with inserted data.
# !hadoop fs -cat /user/itv020752/warehouse/itv020752_db.db/table_demoa/part-00001-180fa8f4-64de-47ce-8056-fda67457a592-c000.csv
# 1112,2013-07-25 00:00:00.0,12111,COMPLETE

##########################################################################################
# placing the file manually for managed, and ext tables: 
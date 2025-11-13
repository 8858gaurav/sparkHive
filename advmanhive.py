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

df = spark.read.csv("data/orders.csv", header=True, inferSchema=True)
spark.sql("""
CREATE TABLE itv020752_db.table_demod (
    order_id STRING, 
    order_date STRING, 
    customer_id STRING, 
    order_status STRING
)
USING CSV
""")

df.createOrReplaceTempView("orders")
spark.sql("insert into itv020752_db.table_demod select * from orders")

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
#  Table(name='table_demob', database='itv020752_db', description=None, tableType='EXTERNAL', isTemporary=False),
# Table(name='table_democ', database='itv020752_db', description=None, tableType='EXTERNAL', isTemporary=False),
# Table(name='table_demod', database='itv020752_db', description=None, tableType='MANAGED', isTemporary=False)]

spark.sql("describe extended table_demod").show(truncate = False)
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
# |Table                       |table_demod                                                                       |       |
# |Owner                       |itv020752                                                                         |       |
# |Created Time                |Thu Nov 13 12:42:34 EST 2025                                                      |       |
# |Last Access                 |UNKNOWN                                                                           |       |
# |Created By                  |Spark 3.1.2                                                                       |       |
# |Type                        |MANAGED                                                                           |       |
# |Provider                    |CSV                                                                               |       |
# |Location                    |hdfs://m01.itversity.com:9000/user/itv020752/warehouse/itv020752_db.db/table_demod|       |
# |Serde Library               |org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe                                |       |
# |InputFormat                 |org.apache.hadoop.mapred.SequenceFileInputFormat                                  |       |
# |OutputFormat                |org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat                         |       |
# +----------------------------+----------------------------------------------------------------------------------+-------+


spark.sql("select count(*) from table_demod").show()
# +--------+
# |count(1)|
# +--------+
# |     104|
# +--------+

# !hadoop fs -ls warehouse/itv020752_db.db/table_demod/
# Found 2 items
# -rw-r--r--   3 itv020752 supergroup          0 2025-11-13 12:42 warehouse/itv020752_db.db/table_demod/_SUCCESS
# -rw-r--r--   3 itv020752 supergroup       4289 2025-11-13 12:42 warehouse/itv020752_db.db/table_demod/part-00000-fbfa47a1-265f-4b5e-87f9-afb76c3f352b-c000.csv

############################################################################
# palcing a file with header columns
# !hadoop fs -put /home/itv020752/Data/Datasets/orders_with_header.csv warehouse/itv020752_db.db/table_demod/
# !hadoop fs -ls warehouse/itv020752_db.db/table_demod/
# Found 3 items
# -rw-r--r--   3 itv020752 supergroup          0 2025-11-13 12:42 warehouse/itv020752_db.db/table_demod/_SUCCESS
# -rw-r--r--   3 itv020752 supergroup         82 2025-11-13 12:47 warehouse/itv020752_db.db/table_demod/orders_with_header.csv
# -rw-r--r--   3 itv020752 supergroup       4289 2025-11-13 12:42 warehouse/itv020752_db.db/table_demod/part-00000-fbfa47a1-265f-4b5e-87f9-afb76c3f352b-c000.csv

spark.sql("refresh table table_demod")
spark.sql("select count(*) from table_demod").show()
# +--------+
# |count(1)|
# +--------+
# |     105|
# +--------+

# !hadoop fs -ls warehouse/itv020752_db.db/table_demod/
# Found 2 items
# -rw-r--r--   3 itv020752 supergroup       4333 2025-11-13 12:10 data/data2/orders.csv
# -rw-r--r--   3 itv020752 supergroup         82 2025-11-13 12:14 data/data2/orders_with_header.csv

#############################################################################

# palcing a file without header columns
# !hadoop fs -put /home/itv020752/Data/Datasets/orders_without_header.csv warehouse/itv020752_db.db/table_demod/
# !hadoop fs -ls warehouse/itv020752_db.db/table_demod/
# Found 4 items
# -rw-r--r--   3 itv020752 supergroup          0 2025-11-13 12:53 warehouse/itv020752_db.db/table_demod/_SUCCESS
# -rw-r--r--   3 itv020752 supergroup         82 2025-11-13 12:53 warehouse/itv020752_db.db/table_demod/orders_with_header.csv
# -rw-r--r--   3 itv020752 supergroup         37 2025-11-13 12:54 warehouse/itv020752_db.db/table_demod/orders_without_header.csv
# -rw-r--r--   3 itv020752 supergroup       4289 2025-11-13 12:53 warehouse/itv020752_db.db/table_demod/part-00000-d39cd857-c89d-4c93-b4e1-7e0825c94846-c000.csv

spark.sql("refresh table table_demod")
spark.sql("select count(*) from table_demod").show()
# +--------+
# |count(1)|
# +--------+
# |     107|
# +--------+

# !hadoop fs -ls warehouse/itv020752_db.db/table_demod/
# Found 4 items
# -rw-r--r--   3 itv020752 supergroup          0 2025-11-13 12:53 warehouse/itv020752_db.db/table_demod/_SUCCESS
# -rw-r--r--   3 itv020752 supergroup         82 2025-11-13 12:53 warehouse/itv020752_db.db/table_demod/orders_with_header.csv
# -rw-r--r--   3 itv020752 supergroup         37 2025-11-13 12:54 warehouse/itv020752_db.db/table_demod/orders_without_header.csv
# -rw-r--r--   3 itv020752 supergroup       4289 2025-11-13 12:53 warehouse/itv020752_db.db/table_demod/part-00000-d39cd857-c89d-4c93-b4e1-7e0825c94846-c000.csv

spark.sql("refresh table table_demod")
spark.sql("select count(*) from table_demod").show()
# +--------+
# |count(1)|
# +--------+
# |     107|
# +--------+

# !hadoop fs -ls warehouse/itv020752_db.db/table_demod/
# Found 4 items
# -rw-r--r--   3 itv020752 supergroup          0 2025-11-13 12:53 warehouse/itv020752_db.db/table_demod/_SUCCESS
# -rw-r--r--   3 itv020752 supergroup         82 2025-11-13 12:53 warehouse/itv020752_db.db/table_demod/orders_with_header.csv
# -rw-r--r--   3 itv020752 supergroup         37 2025-11-13 12:54 warehouse/itv020752_db.db/table_demod/orders_without_header.csv
# -rw-r--r--   3 itv020752 supergroup       4289 2025-11-13 12:53 warehouse/itv020752_db.db/table_demod/part-00000-d39cd857-c89d-4c93-b4e1-7e0825c94846-c000.csv


# !hadoop fs -cat warehouse/itv020752_db.db/table_demod/orders_with_header.csv
# order_id,order_date,customer_id,order_status
# 1,2013-07-25 00:00:00.0,11599,CLOSED


# !hadoop fs -cat warehouse/itv020752_db.db/table_demod/orders_without_header.csv
# 1,2013-07-25 00:00:00.0,11599,CLOSED

# !hadoop fs -cat warehouse/itv020752_db.db/table_demod/part-00000-d39cd857-c89d-4c93-b4e1-7e0825c94846-c000.csv | head 
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

spark.sql("select * from table_demod where order_id = '1'").show()

# +--------+--------------------+-----------+------------+
# |order_id|          order_date|customer_id|order_status|
# +--------+--------------------+-----------+------------+
# |       1|2013-07-25 00:00:...|      11599|      CLOSED|
# |       1|2013-07-25 00:00:...|      11599|      CLOSED|
# |       1|2013-07-25 00:00:...|      11599|      CLOSED|
# +--------+--------------------+-----------+------------+

#############################################################################

# !hadoop fs -put /home/itv020752/Data/Datasets/orders_with_header2.csv warehouse/itv020752_db.db/table_demod/
# !hadoop fs -ls warehouse/itv020752_db.db/table_demod/
# Found 5 items
# -rw-r--r--   3 itv020752 supergroup          0 2025-11-13 12:53 warehouse/itv020752_db.db/table_demod/_SUCCESS
# -rw-r--r--   3 itv020752 supergroup         82 2025-11-13 12:53 warehouse/itv020752_db.db/table_demod/orders_with_header.csv
# -rw-r--r--   3 itv020752 supergroup         82 2025-11-13 12:57 warehouse/itv020752_db.db/table_demod/orders_with_header2.csv
# -rw-r--r--   3 itv020752 supergroup         37 2025-11-13 12:54 warehouse/itv020752_db.db/table_demod/orders_without_header.csv
# -rw-r--r--   3 itv020752 supergroup       4289 2025-11-13 12:53 warehouse/itv020752_db.db/table_demod/part-00000-d39cd857-c89d-4c93-b4e1-7e0825c94846-c000.csv


spark.sql("refresh table table_demod")
spark.sql("select count(*) from table_demod").show()
# +--------+
# |count(1)|
# +--------+
# |     109|
# +--------+

# !hadoop fs -ls warehouse/itv020752_db.db/table_demod/
# Found 5 items
# -rw-r--r--   3 itv020752 supergroup          0 2025-11-13 12:53 warehouse/itv020752_db.db/table_demod/_SUCCESS
# -rw-r--r--   3 itv020752 supergroup         82 2025-11-13 12:53 warehouse/itv020752_db.db/table_demod/orders_with_header.csv
# -rw-r--r--   3 itv020752 supergroup         82 2025-11-13 12:57 warehouse/itv020752_db.db/table_demod/orders_with_header2.csv
# -rw-r--r--   3 itv020752 supergroup         37 2025-11-13 12:54 warehouse/itv020752_db.db/table_demod/orders_without_header.csv
# -rw-r--r--   3 itv020752 supergroup       4289 2025-11-13 12:53 warehouse/itv020752_db.db/table_demod/part-00000-d39cd857-c89d-4c93-b4e1-7e0825c94846-c000.csv

spark.sql("select * from table_demod where order_id = '1'").show()
# +--------+--------------------+-----------+------------+
# |order_id|          order_date|customer_id|order_status|
# +--------+--------------------+-----------+------------+
# |       1|2013-07-25 00:00:...|      11599|      CLOSED|
# |       1|2013-07-25 00:00:...|      11599|      CLOSED|
# |       1|2013-07-25 00:00:...|      11599|      CLOSED|
# |       1|2013-07-25 00:00:...|      11599|      CLOSED|
# +--------+--------------------+-----------+------------+

# !hadoop fs -cat warehouse/itv020752_db.db/table_demod/orders_with_header2.csv
# order_id,order_date,customer_id,order_status
# 1,2013-07-25 00:00:00.0,11599,CLOSED


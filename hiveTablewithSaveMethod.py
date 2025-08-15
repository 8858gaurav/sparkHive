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
           .appName("hiveTable") \
           .config("spark.shuffle.useOldFetchProtocol","true") \
           .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse") \
           .enableHiveSupport() \
           .master("yarn") \
           .getOrCreate()
    
    orders_schema = "order_id long , order_date string, customer_id long,order_status string"
    orders_df = spark.read \
    .format("csv") \
    .schema(orders_schema) \
    .load("/public/trendytech/retail_db/ordersnew/*")

    spark.sql("create database itv020752_hivetable_savemethod").show()

    # !hadoop fs -ls /user/itv020752/warehouse/
    # Found 7 items
    # drwxr-xr-x   - itv020752 supergroup          0 2025-07-17 10:05 /user/itv020752/warehouse/itv020752_db.db
    # drwxr-xr-x   - itv020752 supergroup          0 2025-08-15 06:29 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db
    # drwxr-xr-x   - itv020752 supergroup          0 2025-07-17 10:15 /user/itv020752/warehouse/itv020752_partitioning.db
    # drwxr-xr-x   - itv020752 supergroup          0 2025-08-15 01:23 /user/itv020752/warehouse/misgaurav_101.db
    # drwxr-xr-x   - itv020752 supergroup          0 2025-08-14 09:43 /user/itv020752/warehouse/misgaurav_acid.db
    # drwxr-xr-x   - itv020752 supergroup          0 2025-08-13 06:47 /user/itv020752/warehouse/misgaurav_hive.db
    # drwxr-xr-x   - itv020752 supergroup          0 2025-08-13 07:17 /user/itv020752/warehouse/misgaurav_hive_new.db

    orders_df.write.mode("overwrite").bucketBy(4, "order_id").saveAsTable("itv020752_hivetable_savemethod.orders")

    # !hadoop fs -ls /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/
    # Found 1 items
    # drwxr-xr-x   - itv020752 supergroup          0 2025-08-15 06:32 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders
    # !hadoop fs -ls /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders
    # Found 93 items
    # -rw-r--r--   3 itv020752 supergroup          0 2025-08-15 06:32 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/_SUCCESS
    # -rw-r--r--   3 itv020752 supergroup    2731863 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00000-e69e3046-5abe-4370-baad-026b99f2cf42_00000.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2649901 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00000-e69e3046-5abe-4370-baad-026b99f2cf42_00001.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    1431090 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00000-e69e3046-5abe-4370-baad-026b99f2cf42_00002.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2720375 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00000-e69e3046-5abe-4370-baad-026b99f2cf42_00003.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2664829 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00001-e69e3046-5abe-4370-baad-026b99f2cf42_00000.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2626496 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00001-e69e3046-5abe-4370-baad-026b99f2cf42_00001.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    1273222 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00001-e69e3046-5abe-4370-baad-026b99f2cf42_00002.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2644260 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00001-e69e3046-5abe-4370-baad-026b99f2cf42_00003.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2733338 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00002-e69e3046-5abe-4370-baad-026b99f2cf42_00000.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2666520 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00002-e69e3046-5abe-4370-baad-026b99f2cf42_00001.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    1398081 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00002-e69e3046-5abe-4370-baad-026b99f2cf42_00002.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2700759 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00002-e69e3046-5abe-4370-baad-026b99f2cf42_00003.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2665076 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00003-e69e3046-5abe-4370-baad-026b99f2cf42_00000.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2604636 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00003-e69e3046-5abe-4370-baad-026b99f2cf42_00001.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    1270988 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00003-e69e3046-5abe-4370-baad-026b99f2cf42_00002.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2648267 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00003-e69e3046-5abe-4370-baad-026b99f2cf42_00003.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2741356 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00004-e69e3046-5abe-4370-baad-026b99f2cf42_00000.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2648598 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00004-e69e3046-5abe-4370-baad-026b99f2cf42_00001.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    1493413 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00004-e69e3046-5abe-4370-baad-026b99f2cf42_00002.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2707160 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00004-e69e3046-5abe-4370-baad-026b99f2cf42_00003.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2722588 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00005-e69e3046-5abe-4370-baad-026b99f2cf42_00000.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2709466 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00005-e69e3046-5abe-4370-baad-026b99f2cf42_00001.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    1413849 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00005-e69e3046-5abe-4370-baad-026b99f2cf42_00002.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2716621 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00005-e69e3046-5abe-4370-baad-026b99f2cf42_00003.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2569842 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00006-e69e3046-5abe-4370-baad-026b99f2cf42_00000.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2468965 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00006-e69e3046-5abe-4370-baad-026b99f2cf42_00001.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    1275771 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00006-e69e3046-5abe-4370-baad-026b99f2cf42_00002.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2547385 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00006-e69e3046-5abe-4370-baad-026b99f2cf42_00003.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2450227 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00007-e69e3046-5abe-4370-baad-026b99f2cf42_00000.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2419410 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00007-e69e3046-5abe-4370-baad-026b99f2cf42_00001.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    1095054 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00007-e69e3046-5abe-4370-baad-026b99f2cf42_00002.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2430190 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00007-e69e3046-5abe-4370-baad-026b99f2cf42_00003.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2456787 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00008-e69e3046-5abe-4370-baad-026b99f2cf42_00000.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2417471 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00008-e69e3046-5abe-4370-baad-026b99f2cf42_00001.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    1079212 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00008-e69e3046-5abe-4370-baad-026b99f2cf42_00002.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2419439 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00008-e69e3046-5abe-4370-baad-026b99f2cf42_00003.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2454590 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00009-e69e3046-5abe-4370-baad-026b99f2cf42_00000.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2418939 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00009-e69e3046-5abe-4370-baad-026b99f2cf42_00001.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    1040225 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00009-e69e3046-5abe-4370-baad-026b99f2cf42_00002.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2428524 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00009-e69e3046-5abe-4370-baad-026b99f2cf42_00003.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2454810 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00010-e69e3046-5abe-4370-baad-026b99f2cf42_00000.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2417907 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00010-e69e3046-5abe-4370-baad-026b99f2cf42_00001.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    1069438 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00010-e69e3046-5abe-4370-baad-026b99f2cf42_00002.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2415640 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00010-e69e3046-5abe-4370-baad-026b99f2cf42_00003.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2453186 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00011-e69e3046-5abe-4370-baad-026b99f2cf42_00000.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2414517 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00011-e69e3046-5abe-4370-baad-026b99f2cf42_00001.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    1058922 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00011-e69e3046-5abe-4370-baad-026b99f2cf42_00002.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2429821 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00011-e69e3046-5abe-4370-baad-026b99f2cf42_00003.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2455112 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00012-e69e3046-5abe-4370-baad-026b99f2cf42_00000.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2416274 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00012-e69e3046-5abe-4370-baad-026b99f2cf42_00001.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    1088162 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00012-e69e3046-5abe-4370-baad-026b99f2cf42_00002.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2433006 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00012-e69e3046-5abe-4370-baad-026b99f2cf42_00003.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2454720 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00013-e69e3046-5abe-4370-baad-026b99f2cf42_00000.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2418858 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00013-e69e3046-5abe-4370-baad-026b99f2cf42_00001.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    1055293 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00013-e69e3046-5abe-4370-baad-026b99f2cf42_00002.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2430389 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00013-e69e3046-5abe-4370-baad-026b99f2cf42_00003.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2401430 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00014-e69e3046-5abe-4370-baad-026b99f2cf42_00000.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2366419 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00014-e69e3046-5abe-4370-baad-026b99f2cf42_00001.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    1049940 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00014-e69e3046-5abe-4370-baad-026b99f2cf42_00002.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2381322 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00014-e69e3046-5abe-4370-baad-026b99f2cf42_00003.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2699213 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00015-e69e3046-5abe-4370-baad-026b99f2cf42_00000.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2504190 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00015-e69e3046-5abe-4370-baad-026b99f2cf42_00001.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2405251 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00015-e69e3046-5abe-4370-baad-026b99f2cf42_00002.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2623880 2025-08-15 06:32 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00015-e69e3046-5abe-4370-baad-026b99f2cf42_00003.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2830874 2025-08-15 06:32 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00016-e69e3046-5abe-4370-baad-026b99f2cf42_00000.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    1448062 2025-08-15 06:32 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00016-e69e3046-5abe-4370-baad-026b99f2cf42_00001.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2828971 2025-08-15 06:32 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00016-e69e3046-5abe-4370-baad-026b99f2cf42_00002.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    1774401 2025-08-15 06:32 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00016-e69e3046-5abe-4370-baad-026b99f2cf42_00003.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2706893 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00017-e69e3046-5abe-4370-baad-026b99f2cf42_00000.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2285079 2025-08-15 06:32 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00017-e69e3046-5abe-4370-baad-026b99f2cf42_00001.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2433466 2025-08-15 06:32 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00017-e69e3046-5abe-4370-baad-026b99f2cf42_00002.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    2542959 2025-08-15 06:32 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00017-e69e3046-5abe-4370-baad-026b99f2cf42_00003.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup     826709 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00018-e69e3046-5abe-4370-baad-026b99f2cf42_00000.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    1003719 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00018-e69e3046-5abe-4370-baad-026b99f2cf42_00001.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    1056984 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00018-e69e3046-5abe-4370-baad-026b99f2cf42_00002.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    1186571 2025-08-15 06:32 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00018-e69e3046-5abe-4370-baad-026b99f2cf42_00003.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    1132018 2025-08-15 06:32 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00019-e69e3046-5abe-4370-baad-026b99f2cf42_00000.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    1249692 2025-08-15 06:32 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00019-e69e3046-5abe-4370-baad-026b99f2cf42_00001.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup     987002 2025-08-15 06:32 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00019-e69e3046-5abe-4370-baad-026b99f2cf42_00002.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    1241315 2025-08-15 06:32 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00019-e69e3046-5abe-4370-baad-026b99f2cf42_00003.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    1462540 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00020-e69e3046-5abe-4370-baad-026b99f2cf42_00000.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    1140927 2025-08-15 06:32 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00020-e69e3046-5abe-4370-baad-026b99f2cf42_00001.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    1580133 2025-08-15 06:32 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00020-e69e3046-5abe-4370-baad-026b99f2cf42_00002.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup    1759284 2025-08-15 06:32 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00020-e69e3046-5abe-4370-baad-026b99f2cf42_00003.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup     229775 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00021-e69e3046-5abe-4370-baad-026b99f2cf42_00000.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup     207969 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00021-e69e3046-5abe-4370-baad-026b99f2cf42_00001.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup     376516 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00021-e69e3046-5abe-4370-baad-026b99f2cf42_00002.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup     279884 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00021-e69e3046-5abe-4370-baad-026b99f2cf42_00003.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup      36313 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00022-e69e3046-5abe-4370-baad-026b99f2cf42_00000.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup      33463 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00022-e69e3046-5abe-4370-baad-026b99f2cf42_00001.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup      37328 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00022-e69e3046-5abe-4370-baad-026b99f2cf42_00002.c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup      31641 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db/orders/part-00022-e69e3046-5abe-4370-baad-026b99f2cf42_00003.c000.snappy.parquet

    # hive> select * from itv020752_hivetable_savemethod.orders limit 5;
    # OK
    # 2483    2013-08-07 00:00:00.0   10453   NULL
    # 30484   2014-01-30 00:00:00.0   2876    NULL
    # 30486   2014-01-30 00:00:00.0   1151    NULL
    # 2491    2013-08-07 00:00:00.0   247     NULL
    # 30519   2014-01-30 00:00:00.0   12205   NULL
    # Time taken: 29.846 seconds, Fetched: 5 row(s)
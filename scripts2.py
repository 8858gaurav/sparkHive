# it's your directory where temporary if required, your hive data will be stored.
# <property>
#     <name>hive.exec.local.scratchdir</name>
#     <value>/tmp/${user.name}</value>
#     <description>Local scratch space for Hive jobs</description>
#   </property>

# how to set up a MYSQL in hive
# this part is pending (under MYSQL, we'll be able to see hive metastore database)

# hive> set hive.execution.engine;
# hive.execution.engine=mr
# hive> create database misgaurav_dummy;
# this will create a database under /user/hive/warehouse in hdfs; but for this lab we don't have an aceess to write it here.
# Execution Error, return code 1 from org.apache.hadoop.hive.ql.exec.DDLTask. MetaException(message:Got exception: org.apache.hadoop.
# security.AccessControlException Permission denied: user=itv020752, access=WRITE, inode="/user/hive/warehouse":hive:students:drwxr-xr-x
# the default location of any db or table created in Hive would be /user/hive/warehouse.


# if we want to change the default directory, then use the below one:
# hive> create database misgaurav_temp location '/user/hive/temp';
# this will create a database under /user/hive/temp in hdfs; but for this lab we don't have an aceess to write it here.
# FAILED: Execution Error, return code 1 from org.apache.hadoop.hive.ql.exec.DDLTask. MetaException(message:Got exception: org.apache.hadoop.
# security.AccessControlException Permission denied: user=itv020752, access=WRITE, inode="/user/hive":hdfs:supergroup:drwxr-xr-x


#!/usr/bin/env python
# coding: utf-8
#SPARK_MAJOR_VERSION=2 spark-submit --conf spark.hadoop.fs.permissions.umask-mode=000
--master yarn --deploy-mode client --num-executors 5 --executor-cores 4 --executor-memory 10GB
--driver-memory 10G --conf spark.port.maxRetries=50 --conf spark.debug.maxToStringFields=1000 
# # Author: Mihir Singh
# system argv 1 path were json exist
# 2 namenode path of onprem
# 3 namenode path of cloud
# # import necessary spark and python libraries

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys
spark=SparkSession.builder.appName("transpose").enableHiveSupport().getOrCreate()


# # read the json files change path as per need

# In[2]:


df1=spark.read.json("{}/onprem.json".format(sys.argv[1]))
df2=spark.read.json("{}/cloud.json".format(sys.argv[1]))


# # selecting common database and table that comes as column heading in DataFrame

# In[3]:


common=list(set(df1.columns).intersection(set(df2.columns)))
uncommon_df1=list(set(df1.columns)-set(common))
tables=[]
for a in uncommon_df1:
    print(a.replace('-','.'))
    tables.append(spark.sql("show create table {}".format(a.replace('-','.'))).collect())

#name_node on prem
pattern1="{}".format(sys.argv[2])
#name_node on cloud
pattern2="{}".format(sys.argv[3])
myquery=[]
for query in tables:
    for tmp in query:
        #print(tmp.createtab_stmt)
        a=str(tmp.createtab_stmt).replace(pattern1,"").replace(pattern2,"")
        a="{};".format(a[0:a.index("TBLPROPERTIES")])
        print(a)
        myquery.append(a)


with open('ddlquery.hql', 'w') as f:
    for item in myquery:
        f.write("%s\n" % item)

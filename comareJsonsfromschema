#!/usr/bin/env python
# coding: utf-8
# Author: Mihir Singh

# ## import necessary packages n pass unix(source) and hadoop(destination location) as system argument 1,2


from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import sys
spark=SparkSession.builder.appName("create_json").enableHiveSupport().getOrCreate()


# ## create a list of database value


mydb=spark.sql("show databases").collect()


db=[]
for mydb in mydb:
    db.append(mydb.databaseName)



# ## the below code takes in database name -- and collect all tables inside it as dbname.table name
mytables=[]
for db in db:
    print("database in use",db)
    spark.sql("use {}".format(db))
    tables=spark.sql("show tables")
    param=tables.select(concat(col("database"),lit("."),col('tableName')).alias("tables")).collect()
    for a in param:
        print("tables",a.tables)
        mytables.append(a.tables)




# ## create dict with key as dbname-tablename and value as 'column_name|datatype'
myschemas1={}
c=0
for b in mytables:
    print(str(b).replace(".","-"))
    try:       
        myschemas1[str(b).replace(".","-")]=list(spark.sql("desc {}".format(b)).select(concat(col("col_name"),lit("|"),col("data_type"))).collect())
    except:
        print("Permission error")
        break



# ## import json package to dump dict to json



import json
with open("{}/onprem.json".format(sys.argv[1]), "w") as fp:
    json.dump(myschemas1 , fp)




#with open("{}/cloud.json".format(sys.argv[1]), "w") as fp:
#    json.dump(myschemas2 , fp)



# ## comment uncomment as per environment to upload file to hdfs

# In[10]:
import os
os.system("hadoop fs -put -f {}/onprem.json {}/onprem.json".format(sys.argv[1],sys.argv[2]))
#os.system("hadoop fs -put -f {}/cloud.json {}/cloud.json".format(sys.argv[1],sys.argv[2]))


    

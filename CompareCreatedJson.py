#!/usr/bin/env python
# coding: utf-8

# # Author: Mihir Singh
# system argv 1 -> location of json file 
# system arv 2 -> location where final alter command are created in hql
# 
# # import necessary spark and python libraries

# In[1]:

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark=SparkSession.builder.appName("transpose").enableHiveSupport().getOrCreate()


# # read the json files change path as per need

# enter namenode
namenode=""

df1=spark.read.json("{}/onprem.json".format(sys.argv[1]))
df2=spark.read.json("{}/cloud.json".format(sys.argv[1]))


# # selecting common database and table that comes as column heading in DataFrame

# In[3]:


common=list(set(df1.columns).intersection(set(df2.columns)))
df1=df1.select(*common)
df2=df2.select(*common)


# # convert row values from Named Tuple to List

# In[4]:


cols1=df1.columns
cols2=df2.columns
mylist1=[]
mylist2=[]
print(cols1,cols2)
for x in range(len(cols1)):
    mylist1.append(df1.head()[x])
    mylist2.append(df2.head()[x])


# # Create Dataframe from the rows and columns transposed

# In[5]:


columns_df1=spark.createDataFrame([(l,) for l in mylist1], ['Columns_df1'])
columns_df2 = spark.createDataFrame([(l,) for l in mylist2], ['Columns_df2'])
db=spark.createDataFrame([(l,) for l in cols1], ['Dbname'])


# # join the columns associated with same databases

# In[6]:


from pyspark.sql.functions import *
columns_df1 = columns_df1.withColumn("row_idx", monotonically_increasing_id())
columns_df2 = columns_df2.withColumn("row_idx", monotonically_increasing_id())
db = db.withColumn("row_idx", monotonically_increasing_id())
final_df = db.join(columns_df1, columns_df1.row_idx == db.row_idx).join(columns_df2,columns_df2.row_idx==db.row_idx).drop("row_idx")


# # function that accepts pattern and list are returns 2 list with matching pattern value

# In[7]:


def matcher(pattern,my_list1,my_list):
    matching1 = [s for s in my_list1 if any(xs in s for xs in pattern)]
    matching2 = [s for s in my_list1 if any(xs in s for xs in pattern)]
    return matching1,matching2


# 
# # Function that matches list on following parameters

# # Step1: Check if the table is partitioned or not

# # Step2: Check if the lists have equal columns

# # Step3: If above criteria match the compare schema columns for there order

# In[8]:


def compare(list1,list2):
    emptylist=[]
    flag=0
    pop_flag=0
    pattern=['#']
    p,q=matcher(pattern,list1,list2)
    print(p,q)
    if (len(p)>0):
        popindex=list1.index(p[0])
        list1=list1[0:popindex]
    if (len(q)>0):
        popindex=list2.index(q[0])
        list2=list2[0:popindex]
    if (len(list2)<len(list1)):
        print("list1 has less columns")
        print(sorted(set(list1)-set(list2)))
        return list1
    elif(len(list2)==len(list1)):pass
    else:
        print("list2 has more columns")
        print(sorted(set(list1)-set(list2)))
        return list1
    for l1,l2 in zip(list1,list2):
        if(l1==l2):
            print("compare block if : l1 ->{} l2->{}".format(l1,l2))
            pass
        else:
            print("compare block else : l1 ->{} l2->{}".format(l1,l2))
            flag=1
            break
            
    if (flag==1):
        return list1
    else:
        return emptylist


# # final code that passes schema and column ,and generates a list with queries to be executed

# In[9]:


d_query_rename_cols=[]
for cols in cols1:
    print("comparing table:{}".format(cols))
    temp1=final_df.select('dbName','Columns_df1').filter(col('dbName')==cols).collect()
    temp2=final_df.select('dbName','Columns_df2').filter(col('dbName')==cols).collect()
    for t1,t2 in zip(temp1,temp2):
        l1=[''.join(x) for x in t1[1]]
        l2=[''.join(x) for x in t2[1]]
        print("l1 ->{} l2->{}".format(l1,l2))
        p=compare(l1,l2)
        if len(p)!=0: 
            print(("Alter table {} replace columns {};".format(str(t1[0]).replace("-","."),p)).replace('|',' ').replace('[','(').replace(']',')').replace("'",""))
            d_query_rename_cols.append(str("Alter table {} replace columns {};".format(str(t2[0]).replace("-","."),p)).replace('|',' ').replace("# Partition Information ,","").replace("# col_name data_type,","").replace('[','(').replace(']',')').replace("'",""))
        else:
            print("alter not required")


# # Dump the array  to hql file to executed manually.


with open('{}/query.hql'.format(sys.argv[2]), 'w') as f:
    for item in d_query_rename_cols:
        f.write("%s\n" % item)

#!/usr/bin/env python
# coding: utf-8

# In[13]:


import pyspark
from pyspark.sql import SparkSession


# In[14]:


import pandas


# In[15]:


#pip install openpyxl


# In[16]:


spark = SparkSession.builder.master("local[1]").appName("Test").enableHiveSupport().getOrCreate()


# In[17]:


df=spark.sql("select * from hive_input_output.click_stream_json")


# In[18]:


#df=spark.read.option("header","true").json("hdfs://0.0.0.0:9000/user/hive/warehouse/hive_input_output.db/click_stream_json/Data.json")


# In[19]:


df.show()


# In[20]:


df.createOrReplaceTempView("Day_Wise")


# In[21]:


df1=spark.sql("select LogDate, count(UserID) as Day_Wise from Day_Wise group by LogDate order by Day_Wise desc")


# In[22]:


df1.show()


# In[25]:


df1.write.mode('overwrite').format('csv').option('header','true').saveAsTable("spark_output.Day_Wise_Analysis")


# In[27]:


df1.toPandas().to_excel('/home/suraj/Output/Day_Wise_Analysis.xlsx', sheet_name = 'Sheet1', index = False)


# In[ ]:





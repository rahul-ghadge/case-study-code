#!/usr/bin/env python
# coding: utf-8

# In[3]:


import pyspark
from pyspark.sql import SparkSession


# In[4]:


spark = SparkSession.builder.master("local[1]").appName("Test").enableHiveSupport().getOrCreate()


# In[5]:


from pyspark.sql.functions import *


# In[6]:


import pandas


# In[7]:


#pip install openpyxl


# In[8]:


df=spark.sql("select * from hive_input_output.click_stream_json")


# In[9]:


df.show()


# In[10]:


df1=spark.sql("select Location, count(UserID) as location_count from hive_input_output.click_stream_json group by Location")


# In[11]:


df1.show()


# In[12]:


df1.write.mode('overwrite').format('csv').option('header','true').saveAsTable('spark_output.Location_Analysis')


# In[13]:


df1.toPandas().to_excel('/home/suraj/Output/Location_Analysis.xlsx', sheet_name = 'Sheet1', index = False)


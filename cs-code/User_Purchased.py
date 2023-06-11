#!/usr/bin/env python
# coding: utf-8

# In[4]:


import pyspark
from pyspark.sql import SparkSession


# In[5]:


import pandas


# In[6]:


#pip install openpyxl


# In[7]:


spark = SparkSession.builder.master("local[1]").appName("Test").enableHiveSupport().getOrCreate()


# In[8]:


df=spark.sql("select * from hive_input_output.click_stream_json")


# In[9]:


#df=spark.read.option("header","true").json("hdfs://0.0.0.0:9000/user/hive/warehouse/hive_input_output.db/click_stream_json/Data.json")


# In[10]:


df.show()


# In[11]:


from pyspark.sql.functions import *


# In[12]:


df1 = df.withColumn("item", split(df['URL'], 'item.').getItem(1))


# In[13]:


df1.show()


# In[14]:


df1.createOrReplaceTempView("User_Purchase")


# In[15]:


df2=spark.sql("select User_Name, count(item) as user_purchased from User_Purchase where Action='purchase' group by User_Name")


# In[16]:


df2.show()


# In[17]:


df2.write.mode('overwrite').format('csv').option('header','true').saveAsTable("spark_output.User_Purchased")


# In[18]:


df2.toPandas().to_excel('/home/suraj/Output/User_Purchased.xlsx', sheet_name = 'Sheet1', index = False)


# In[ ]:





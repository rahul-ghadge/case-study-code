#!/usr/bin/env python
# coding: utf-8

# In[18]:


import pyspark
from pyspark.sql import SparkSession


# In[19]:


spark = SparkSession.builder.master("local[1]").appName("Test").enableHiveSupport().getOrCreate()


# In[20]:


import pandas


# In[21]:


#pip install openpyxl


# In[22]:


df=spark.sql("select * from hive_input_output.click_stream_json")


# In[23]:


#df=spark.read.option("header","true").json("hdfs://0.0.0.0:9000/user/hive/warehouse/hive_input_output.db/click_stream_json/Data.json")


# In[24]:


df.show()


# In[25]:


from pyspark.sql.functions import *


# In[26]:


df1 = df.withColumn("item", split(df['URL'], 'item.').getItem(1))


# In[27]:


df1.show()


# In[28]:


df1.createOrReplaceTempView("item")


# In[29]:


df2=spark.sql("select item, count(item) as most_visited from item group by item order by most_visited desc")


# In[30]:


df2.show()


# In[31]:


df2.write.mode("overwrite").format('csv').option('header','true').saveAsTable("spark_output.Most_Visited_Item1")


# In[32]:


df2.toPandas().to_excel('/home/suraj/Output/Most_Visited_Item.xlsx', sheet_name = 'Sheet1', index = False)


# In[ ]:





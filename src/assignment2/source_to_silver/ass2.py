# Databricks notebook source
import requests
import json
from pyspark.sql.types import *
from pyspark.sql.functions import explode, split, current_date

# COMMAND ----------

response_data = requests.get('https://reqres.in/api/users?page=2')
json_data = response_data.json()

# COMMAND ----------

data_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("email", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("avatar", StringType(), True)
])

# COMMAND ----------

custom_schema = StructType([
    StructField("page", IntegerType(), True),
    StructField("per_page", IntegerType(), True),
    StructField("total", IntegerType(), True),
    StructField("total_pages", IntegerType(), True),
    StructField("data", ArrayType(data_schema), True),
    StructField("support", MapType(StringType(), StringType()), True)
])

# COMMAND ----------

df = spark.createDataFrame([json_data], custom_schema)
display(df)


# COMMAND ----------


df = df.drop('page', 'per_page', 'total', 'total_pages', 'support')
display(df)


# COMMAND ----------

df= df.withColumn('data', explode('data'))
display(df)


# COMMAND ----------


df = df.withColumn("id", df.data.id).withColumn('email', df.data.email).withColumn('first_name', df.data.first_name).withColumn('last_name', df.data.last_name).withColumn('aatar', df.data.avatar).drop(df.data)
display(df)

# COMMAND ----------

derived_site_address_df = df.withColumn("site_address",split(df["email"],"@")[1])

# COMMAND ----------

# COMMAND ----------

loaded_date = derived_site_address_df.withColumn('load_date', current_date())

# COMMAND ----------

# COMMAND ----------

# COMMAND ----------

loaded_date.write.format('delta').mode('overwrite').save('dbfs:/FileStore/assignments/question2/site_info/person_info')

# COMMAND ----------


testing_df = spark.read.format('delta').load('dbfs:/FileStore/assignments/question2/site_info/person_info')

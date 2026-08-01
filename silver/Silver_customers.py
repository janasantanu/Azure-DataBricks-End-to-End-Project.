# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql. window import Window

# COMMAND ----------

df=spark.read.format('parquet')\
    .load('abfss://bronze@projecte2e.dfs.core.windows.net/customers')

# COMMAND ----------

df.display()

# COMMAND ----------

df=df.drop('_rescued_data')
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Transformation using split(****)

# COMMAND ----------

df=df.withColumn('Domain',split(col('email'),'@')[1])
df.display()

# COMMAND ----------

df.groupBy('Domain').agg(count('customer_id').alias('total_customers')).sort('total_customers',ascending=False).display()

# COMMAND ----------

df_gmail=df.filter(col("domain")=='gmail.com')
df_gmail.display()

# COMMAND ----------

df=df.withColumn('Full_name',concat(col('first_name'),lit(' '),col('last_name')))
df=df.drop("first_name",'last_name')
df.display()

# COMMAND ----------

df.write.mode('overwrite')\
    .format("delta")\
        .save('abfss://silver@projecte2e.dfs.core.windows.net/customers')

# COMMAND ----------

# MAGIC %md
# MAGIC # creating a table in databricks

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema project_cata.silver

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists project_cata.silver.customers_silver
# MAGIC using delta
# MAGIC location "abfss://silver@projecte2e.dfs.core.windows.net/customers"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from project_cata.silver.customers_silver
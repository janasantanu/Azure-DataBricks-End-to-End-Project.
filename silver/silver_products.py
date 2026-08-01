# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

# MAGIC %md
# MAGIC ## **Data Reading**

# COMMAND ----------

df=spark.read.format('parquet')\
    .load('abfss://bronze@projecte2e.dfs.core.windows.net/products')

# COMMAND ----------

df.display()

# COMMAND ----------

df=df.drop("_rescued_data")
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # **FUNCTION_using SQLudf**

# COMMAND ----------

df.createOrReplaceTempView("products")

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace function project_cata.bronze.discount_func(p_price double)
# MAGIC returns double
# MAGIC language SQL
# MAGIC return p_price*0.90

# COMMAND ----------

# MAGIC %sql
# MAGIC select product_id,price,project_cata.bronze.discount_func(price) as discounted_price from products

# COMMAND ----------

# MAGIC %md
# MAGIC # Using Python

# COMMAND ----------

df=df.withColumn("discounted_price",expr("project_cata.bronze.discount_func(price)"))
df.display()

# COMMAND ----------

df.write.mode('overwrite')\
    .format('delta')\
        .option('path',"abfss://silver@projecte2e.dfs.core.windows.net/products")\
            .save()

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists project_cata.silver.products_silver
# MAGIC using delta
# MAGIC location "abfss://silver@projecte2e.dfs.core.windows.net/products"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from project_cata.silver.products_silver
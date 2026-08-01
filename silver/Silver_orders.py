# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *

# COMMAND ----------

df=spark.read.format('parquet')\
    .load('abfss://bronze@projecte2e.dfs.core.windows.net/orders')

# COMMAND ----------

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # **Drop unnecessary column**

# COMMAND ----------

df=df.drop("_rescued_data")
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **create a new year column**

# COMMAND ----------

from pyspark.sql.functions import *
df=df.withColumn("year",year(col('order_date')))

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Use of window function**

# COMMAND ----------

df1=df.withColumn('flag',dense_rank().over(Window.partitionBy('year').orderBy(desc('total_amount'))))
df1.display()

# COMMAND ----------

df1=df.withColumn('rank_flag',rank().over(Window.partitionBy('year').orderBy(desc('total_amount'))))
df1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## **OOps_class**

# COMMAND ----------

class windows:
    def dense_rank(self,df):
        df_dense_rank=df.withColumn('dense_rank',dense_rank(),over(Window.partitionBy('year').orderBy(desc("total_amount"))))
        return dense_rank
    
    def rank_col(self,df):
        df_rank=df.withColumn('rank',rank().over(Window.partitionBy('year').orderBy(desc("total_amount"))))
        return rank_col
    
    def row_num(self,df):
        df_row_num=df.withColumn('row_num_col',row_number().over(Window.partitionBy('year').orderBy(desc('total_amount'))))
        return df_row_num


# COMMAND ----------

df_new=df

# COMMAND ----------

obj=windows()

# COMMAND ----------

row_flag=obj.row_num(df_new)
row_flag.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data **Writting**

# COMMAND ----------

df.write.format('delta').mode('overwrite').save('abfss://silver@projecte2e.dfs.core.windows.net/orders')

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists project_cata.silver.orders_silver
# MAGIC using delta
# MAGIC location "abfss://silver@projecte2e.dfs.core.windows.net/orders"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from project_cata.silver.orders_silver
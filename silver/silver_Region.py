# Databricks notebook source
# MAGIC %md
# MAGIC # **Data reading from databricks schema**

# COMMAND ----------

df=spark.read.table("project_cata.bronze.regions")
df.display()

# COMMAND ----------

df=df.drop("_rescued_data")
df.display()

# COMMAND ----------

df=df.split

# COMMAND ----------

# MAGIC %md
# MAGIC # **Data writing**

# COMMAND ----------

df.write.mode("overwrite")\
    .format("delta")\
        .save("abfss://silver@projecte2e.dfs.core.windows.net/regions")

# COMMAND ----------

df=spark.read.format("delta")\
    .load('abfss://silver@projecte2e.dfs.core.windows.net/regions')
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists project_cata.silver.regions_silver
# MAGIC using delta
# MAGIC location "abfss://silver@projecte2e.dfs.core.windows.net/regions"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from project_cata.silver.regions_silver
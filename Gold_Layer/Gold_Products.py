# Databricks notebook source
# MAGIC %md
# MAGIC ### **DLT Pipeline**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming Table

# COMMAND ----------

# Expectations
my_rules ={
"rule1" : "product_id IS NOT NULL",
"rule2" : "product_name IS NOT NULL"
}

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

# COMMAND ----------

@dlt.table()
@dlt.except_all_or_drop(my_rules)

def DimProducts_stage():
    df = spark.readstream.table("project_cata.silver.products_silver")
return df


# COMMAND ----------

# MAGIC %md
# MAGIC # **Streaming View**

# COMMAND ----------

@dlt.view
def Dimproducts_view():
    df=spark.readStream.table("Live.DimProducts_stage")
    return df


# COMMAND ----------

# MAGIC %md
# MAGIC **DimProducts**

# COMMAND ----------

dlt.create_streaming_table("DimProducts")


# COMMAND ----------

dlt.apply_changes(
target = "DimProducts",
source = "Live.DimProducts_view",
keys = ["product_id"],
sequence_by = "product_id",
stored_as_scd_type = 2
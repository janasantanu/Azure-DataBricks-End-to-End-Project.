# Databricks notebook source
init_load_flag = int(dbutils.widgets.get("init_load_flag"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Data Reading from source**

# COMMAND ----------

df=spark.sql("select * from project_cata.silver.customers_silver")

# COMMAND ----------

df.limit(15).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Removing_duplicates**

# COMMAND ----------

df=df.dropDuplicates(subset=["customer_id"])
df.limit(10).display()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # **Dividing old vs New records**

# COMMAND ----------

if init_load_flag == 0:
    df_old = spark.sql("""
        SELECT DimCustomerKey,
               customer_id,
               create_date,
               update_date
        FROM project_cata.gold_schema.dimcustomers
    """)
else:
    df_old = spark.sql("""
        SELECT 0 DimCustomerKey,
               0 customer_id,
               0 create_date,
               0 update_date
        FROM project_cata.silver.customers_silver
        WHERE 1 = 0
    """)

# COMMAND ----------

df_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Renamed Column**

# COMMAND ----------

df_old=df_old.withColumnRenamed("DimCustomerKey","old_DimCustomerKey")\
    .withColumnRenamed("customer_id","old_customer_id")\
        .withColumnRenamed("create_date","old_create_date")\
            .withColumnRenamed("update_date","old_update_date")

# COMMAND ----------

# MAGIC %md
# MAGIC **Applying join with the old records**

# COMMAND ----------

df_join=df.join(df_old,df.customer_id==df_old.old_customer_id,"left")

# COMMAND ----------

df_join.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **separating old vs new records**

# COMMAND ----------

df_new=df_join.filter(df_join['old_DimCustomerKey'].isNull())

# COMMAND ----------

df_old=df_join.filter(df_join['old_DimCustomerKey'].isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC # **Preparing df old**

# COMMAND ----------

#dropping all the columns which are not requried

df_old = df_old.drop('old_customer_id','old_update_date')
#Renaming "old_dimCustomerkey" to "customer key"
df_old = df_old.withColumnRenamed("old_DimCustomerKey","Dimcustomerkey")

# Renaming "old_create_date" column to "create date"
df_old = df_old.withColumnRenamed("old_create_date","create_date")
df_old = df_old.withColumn("Create_date",to_timestamp(col("create_date")))

#Recreating "old_update date column"
df_old = df_old.withColumn("update_date",current_timestamp())

# COMMAND ----------

df_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # **Preparing df_new**

# COMMAND ----------

df_new.display()

# COMMAND ----------

#dropping all the columns which are not requried

df_new=df_new.drop('old_DimCustomerKey','old_customer_id','old_update_date','old_create_date')


#Recreating "old_update date","old_current_date" column
df_new=df_new.withColumn("update_date",current_timestamp())
df_new=df_new.withColumn("create_date",current_timestamp())


# COMMAND ----------

df_new.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # **Surrogate_key**

# COMMAND ----------

df_new=df_new.withColumn("DimCustomerKey",monotonically_increasing_id()+lit(1))

# COMMAND ----------

# MAGIC %md
# MAGIC **## Adding max surrogate key**

# COMMAND ----------

if init_load_flag == 1:
    max_surrogate_key =0
else:
    df_maxsur= spark.sql("select max(DimCustomerKey) as max_surrog_key from project_cata.gold_schema.dimcustomers")
    # converting df_maxsur to max_surrogate_key
    max_surrogate_key = df_maxsur.collect()[0]['max_surrog_key']

# COMMAND ----------

df_new = df_new.withColumn("DimCustomerKey",lit(max_surrogate_key)+col("DimCustomerKey"))

# COMMAND ----------

# MAGIC %md
# MAGIC **union of df_old and **df_new****

# COMMAND ----------

df_final = df_new.unionByName(df_old)

# COMMAND ----------

df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **SCD type-1**

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists("project_cata.gold_schema.DimCustomers"):
    dlt_obj = DeltaTable.forPath(spark,"abfss://gold@projecte2e.dfs.core.windows.net/DimCustomers")
    dlt_obj.alias("trg").merge(df_final.alias("src"),"trg.DimCustomerKey = src.DimCustomerkey")\
        .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
                .execute()
else:
    df_final.write.mode("overwrite")\
        .option('path',"abfss://gold@projecte2e.dfs.core.windows.net/DimCustomers")\
        .saveAsTable("project_cata.gold_schema.DimCustomers")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from project_cata.gold_schema.dimcustomers

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES IN project_cata;
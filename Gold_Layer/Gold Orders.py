# Databricks notebook source
# MAGIC %md
# MAGIC **Fact Orders**

# COMMAND ----------

# MAGIC %md
# MAGIC # Data **reading**

# COMMAND ----------

df=spark.sql("select * from project_cata.silver.orders_silver")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from project_cata.default.dimproducts

# COMMAND ----------

df_dimcus = spark.sql("select DimCustomerkey, customer_id as dim_customer_id from project_cata.gold_schema.dimcustomers")

df_dimpro = spark.sql("""
select product_id as DimProductKey,
       product_id as dim_product_id
from project_cata.default.dimproducts
""")

# COMMAND ----------

df_fact = df.join(df_dimcus, df['customer_id'] == df_dimcus['dim_customer_id'],how='left').join(df_dimpro, df
['product_id'] == df_dimpro['dim_product_id' ], how='left')

df_fact_new = df_fact.drop('dim_customer_id','dim_product_id','customer_id','product_id')

# COMMAND ----------

df_fact_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Upsert on fact table**

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists("project_cata.gold_schema.Factorders"):

    dlt_obj = DeltaTable.forName(spark, "project_cata.gold_schema.FactOrders")

    dlt_obj.alias("trg").merge(df_fact_new.alias("src"), "trg.order_id = src.order_id AND trg.DimCustomerkey = src.DimCustomerKey AND trg.DimProductkey = src.DimProductKey")\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()

else:
    df_fact_new.write.format("delta")\
    .option("path","abfss://gold@projecte2e.dfs.core.windows.net/Factorders")\
    .saveAsTable("project_cata.gold_schema.Factorders")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from project_cata.gold_schema.Factorders
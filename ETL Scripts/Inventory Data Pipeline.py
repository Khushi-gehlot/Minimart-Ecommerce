# Databricks notebook source
from datetime import date

today = date.today()
print(today)
path = f"/Volumes/workspace/minimart/inventory/inventory_data_{today}.csv"
print(path)
df = spark.read.csv(path, header=True, inferSchema=True, quote= "\"", multiLine=True)
display(df)


# COMMAND ----------

df = df.dropna(subset=["inventory_id", "product_id", "seller_id"])
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from inventory_data_deltatable

# COMMAND ----------

from delta.tables import DeltaTable
delta_table = DeltaTable.forName(spark, "default.inventory_data_deltatable")
df_new = df

delta_table.alias("target").merge(
    df_new.alias("source"),
    "target.inventory_id = source.inventory_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from inventory_data_deltatable
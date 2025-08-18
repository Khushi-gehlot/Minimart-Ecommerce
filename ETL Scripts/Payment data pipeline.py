# Databricks notebook source
from datetime import date

today = date.today()
print(today)
path = f"/Volumes/workspace/minimart/payment/payment_data_{today}.csv"
print(path)
df = spark.read.csv(path, header=True, inferSchema=True, quote= "\"", multiLine=True)
display(df)

# COMMAND ----------

df = df.dropna(subset=["payment_id"])
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from payment_data_deltatable

# COMMAND ----------

from delta.tables import DeltaTable
delta_table = DeltaTable.forName(spark, "default.payment_data_deltatable")
df_new = df

delta_table.alias("target").merge(
    df_new.alias("source"),
    "target.payment_id = source.payment_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from payment_data_deltatable
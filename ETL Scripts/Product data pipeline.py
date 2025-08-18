# Databricks notebook source
from datetime import date

today = date.today()
print(today)
path = f"/Volumes/workspace/minimart/products/product_data_{today}.csv"
print(path)
df = spark.read.csv(path, header=True, inferSchema=True, quote= "\"", multiLine=True)
display(df)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from products_deltatable

# COMMAND ----------

# display(df)
df = df.dropna(subset=["Products_id", "Category_id", "Price"])
display(df)

# COMMAND ----------

from delta.tables import DeltaTable

delta_table = DeltaTable.forName(spark, "default.products_deltatable")
df_new = df

delta_table.alias("target").merge(
    df_new.alias("source"),
    "target.Products_id = source.Products_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from products_deltatable
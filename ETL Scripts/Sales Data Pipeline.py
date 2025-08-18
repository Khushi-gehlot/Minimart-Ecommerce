# Databricks notebook source


from datetime import date

today = date.today()
print(today)
path = f"/Volumes/workspace/minimart/sales/sales_data_{today}.csv"
print(path)
df = spark.read.csv(path, header=True, inferSchema=True, quote= "\"", multiLine=True)
display(df)

# COMMAND ----------

df = df.dropna(subset=["sales_id", "invoice_no", "customer_id","product_id", "seller_id", "quantity", "unit_price", "discount", "total_amount", "payment_id","country_id"])
display(df)

# COMMAND ----------

from pyspark.sql.functions import to_date, col


df = df.withColumn("parsed_date", to_date(col("sale_date"), "yyyy-MM-dd"))
df = df.filter(col("parsed_date").isNotNull())
df = df.drop("parsed_date")

# COMMAND ----------

from delta.tables import DeltaTable
delta_table = DeltaTable.forName(spark, "default.inventory_data_deltatable")
df_new = df

delta_table.alias("target").merge(
    df_new.alias("source"),
    "target.seller_id = source.seller_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
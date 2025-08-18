# Databricks notebook source
path = "/Volumes/workspace/minimart/seller/seller_data_2025-07-23.csv"

from datetime import date

today = date.today()
print(today)
path = f"/Volumes/workspace/minimart/seller/seller_data_{today}.csv"
print(path)
df = spark.read.csv(path, header=True, inferSchema=True, quote= "\"", multiLine=True)
display(df)

# COMMAND ----------

df = df.dropna(subset=["address", "seller_id"])
display(df)

# COMMAND ----------

from pyspark.sql.functions import col, regexp_extract

email_pattern = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
df = df.withColumn("valid_email", regexp_extract(col("email"), email_pattern, 0) != "")
display(df)

# COMMAND ----------

from pyspark.sql.functions import to_date, col


df = df.withColumn("parsed_date", to_date(col("Signup_date"), "yyyy-MM-dd"))
df = df.filter(col("parsed_date").isNotNull())
df = df.drop("parsed_date")

# COMMAND ----------

from pyspark.sql.functions import col, regexp_extract
contact_pattern = r'^\d{10}$'

df = df.withColumn("valid_contact_no", regexp_extract(col("Phone_no"), contact_pattern, 0) != "")
display(df)

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
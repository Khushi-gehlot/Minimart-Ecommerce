# Databricks notebook source
from datetime import date

today = date.today()
print(today)

df = spark.sql(
    """
    SELECT * FROM workspace.default.customers_data_minimart
    """
)
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from customers_deltatable

# COMMAND ----------

df = df.dropna(subset=["customers_id", "Password"])
display(df)

# COMMAND ----------

from pyspark.sql.functions import col, regexp_extract

email_pattern = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
df = df.withColumn("valid_email", regexp_extract(col("email"), email_pattern, 0) != "")
display(df)

# COMMAND ----------

from pyspark.sql.functions import col, regexp_extract
contact_pattern = r'^\d{10}$'

df = df.withColumn("valid_contact_no", regexp_extract(col("contact_no"), contact_pattern, 0) != "")
display(df)


# COMMAND ----------

from pyspark.sql.functions import to_date, col


df = df.withColumn("parsed_date", to_date(col("Signup_date"), "yyyy-MM-dd"))
df = df.filter(col("parsed_date").isNotNull())
df = df.drop("parsed_date")

# COMMAND ----------

from delta.tables import DeltaTable
delta_table = DeltaTable.forName(spark, "default.customers_deltatable")
df_new = df

delta_table.alias("target").merge(
    df_new.alias("source"),
    "target.customers_id = source.customers_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customers_deltatable

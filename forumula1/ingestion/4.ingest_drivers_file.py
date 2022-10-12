# Databricks notebook source
# MAGIC %md ### Step 1 - read json

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

drivers_schema = StructType(fields=[
                     StructField("driverId", IntegerType(), False),
                     StructField("driverRef", StringType(), True),
                     StructField("number", IntegerType(), True),
                     StructField("code", StringType(), True),
                     StructField("name", StructType(fields=[
                         StructField("forename", StringType(), True),
                         StructField("surname", StringType(), True)
                     ])),
                     StructField("dob", DateType(), True),
                     StructField("nationality", StringType(), True),
                     StructField("url", StringType(), True)
])

# COMMAND ----------

drivers_df = spark.read.schema(drivers_schema).json("/mnt/raw/drivers.json")

# COMMAND ----------

# MAGIC %md ### Step 2 - rename columns

# COMMAND ----------

from pyspark.sql.functions import concat, lit, current_timestamp, col

# COMMAND ----------

drivers_with_columns_df = drivers_df.withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("driverRef", "driver_ref") \
                                    .withColumn("ingestion_date", current_timestamp()) \
                                    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))

# COMMAND ----------

# MAGIC %md ### Step 3 - drop columns

# COMMAND ----------

drivers_final_df = drivers_with_columns_df.drop("url")

# COMMAND ----------

drivers_final_df.write.mode("overwrite").parquet("/mnt/processed/drivers")

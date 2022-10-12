# Databricks notebook source
# MAGIC %md ### Step 1 - read JSON

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING NOT NULL, url STRING"

# COMMAND ----------

constructor_df = spark.read.schema(constructors_schema).json("/mnt/raw/constructors.json")

# COMMAND ----------

# MAGIC %md ### Step 2 - drop column

# COMMAND ----------

constructor_dropped_df = constructor_df.drop(constructor_df.url)

# COMMAND ----------

# MAGIC %md ### Step 3 - rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructor_final_df = constructor_dropped_df \
                        .withColumnRenamed("constructorId", "constructor_id") \
                        .withColumnRenamed("constructorRef", "constructor_ref") \
                        .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md ### Step 4 - write to parquet

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet("/mnt/processed/constructors")

# Databricks notebook source
# MAGIC %md ### Step 1 - read from CSV

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, StringType

# COMMAND ----------

races_schema = StructType([
    StructField("raceId", IntegerType(), False),
    StructField("year", IntegerType(), False),
    StructField("round", IntegerType(), False),
    StructField("circuitId", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("date", StringType(), False),
    StructField("time", StringType(), False),
    StructField("url", StringType(), False),
])

# COMMAND ----------

races_df = spark.read.option("header", True).schema(races_schema).csv("/mnt/raw/races.csv")

# COMMAND ----------

# MAGIC %md ### Step 2 - select columns

# COMMAND ----------

races_selected_df = races_df.drop("url")

# COMMAND ----------

# MAGIC %md ### Step 3 - rename columns

# COMMAND ----------

races_renamed_df = races_selected_df  \
                    .withColumnRenamed("raceId", "race_id") \
                    .withColumnRenamed("year", "race_year") \
                    .withColumnRenamed("circuitId", "circuit_id")

# COMMAND ----------

# MAGIC %md ### Step 4 - transform

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, to_timestamp, col, concat

# COMMAND ----------

races_df_final = races_renamed_df \
                    .withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(" "), col("time")), "yyyy-MM-dd HH:mm:ss")) \
                    .withColumn("ingestion_date", lit(current_timestamp())) \
                    .drop("date") \
                    .drop("time")

# COMMAND ----------

# MAGIC %md ### Step 5 - save to parquet

# COMMAND ----------

races_df_final.write.mode("overwrite").partitionBy("race_year").parquet("/mnt/processed/races")

# Databricks notebook source
# MAGIC %md ###Step 1 - read from csv

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

cicuits_schema = StructType(fields=[
    StructField("circuitId", IntegerType(), False),
    StructField("circuitRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("country", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True),
    StructField("alt", IntegerType(), True),
    StructField("url", StringType(), True),
])

# COMMAND ----------

circuits_df = spark.read \
                .option("Header", True) \
                .schema(cicuits_schema) \
                .csv("/mnt/raw/circuits.csv")

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

# MAGIC %md ### Step 2 - select columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country").alias("race_country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

# MAGIC %md ### Step 3 - rename columns

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
                            .withColumnRenamed("circuitRef", "circuit_ref") \
                            .withColumnRenamed("lat", "latitude") \
                            .withColumnRenamed("lng", "longitude") \
                            .withColumnRenamed("alt", "altitude")

# COMMAND ----------

# MAGIC %md ### Step 4 - add ingestion date column 

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp()) \
                        .withColumn("env", lit("Production"))

# COMMAND ----------

# MAGIC %md ### Step 5 - write to parquet

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet("/mnt/processed/circuts")

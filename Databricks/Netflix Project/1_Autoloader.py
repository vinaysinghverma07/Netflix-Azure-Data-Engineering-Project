# Databricks notebook source
# MAGIC %md
# MAGIC # Incremental Data Loading using AutoLoader

# COMMAND ----------

# MAGIC %sql
    CREATE SCHEMA IF NOT EXISTS netflix_catalog.net_schema;

# COMMAND ----------

    checkpoint_location = "abfss://silver@netflixprojectdlvinay.dfs.core.windows.net/checkpoint"

# COMMAND ----------

# checkpoint_path = "s3://dev-bucket/_checkpoint/dev_table"

    df= (spark.readStream 
        # read spark streaming api which is available in PySpark#  
        .format("cloudFiles")\
        .option("cloudFiles.format", "csv")\
        .option("cloudFiles.schemaLocation", checkpoint_location)\
        .load("abfss://raw@netflixprojectdlvinay.dfs.core.windows.net"))
    # .writeStream
    # .option("checkpointLocation", checkpoint_path)
    # .trigger(availableNow=True)
    # .toTable("dev_catalog.dev_database.dev_table"))

# COMMAND ----------

    display(df)

# COMMAND ----------

# .trigger(processingTime='10 seconds') commented out since we do have that cluster with us
    df.writeStream\
        .option("checkpointLocation", checkpoint_location)\
        .trigger(availableNow=True)\
        .start("abfss://bronze@netflixprojectdlvinay.dfs.core.windows.net/netflix_titles")


# COMMAND ----------

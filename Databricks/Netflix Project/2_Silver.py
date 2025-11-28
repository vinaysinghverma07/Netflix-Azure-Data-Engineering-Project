# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Notebook Lookup Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating Parameters
# MAGIC

# COMMAND ----------

    dbutils.widgets.text("sourcefolder","netflix_directors")
    dbutils.widgets.text("targetfolder","netflix_directors")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Variables

    var_src_folder = dbutils.widgets.get("sourcefolder")
    var_tgt_folder = dbutils.widgets.get("targetfolder")

# COMMAND ----------

    print(var_src_folder)

# COMMAND ----------

    df= spark.read.format("csv")\
        .option("header",True)\
        .option("inferSchema",True)\
        .load(f"abfss://bronze@netflixprojectdlvinay.dfs.core.windows.net/{var_src_folder}")

# COMMAND ----------

    df.display()

# COMMAND ----------

    df.write.format("delta")\
        .mode("append")\
        .option("path",f"abfss://silver@netflixprojectdlvinay.dfs.core.windows.net/{var_tgt_folder}")\
    .save()

# COMMAND ----------


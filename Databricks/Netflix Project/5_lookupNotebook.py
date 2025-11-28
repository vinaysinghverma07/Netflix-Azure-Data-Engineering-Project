# Databricks notebook source
# Creating Lookup Notebook for creating the workflow to auto-run the job on weekday.
# MAGIC For this we have to first create the paramaters

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating Parameters

# COMMAND ----------

    dbutils.widgets.text("weekday","7")

# COMMAND ----------

    var = int(dbutils.widgets.get("weekday"))
# print(type(var))

# COMMAND ----------

    dbutils.jobs.taskValues.set(key="weekoutput", value=var)
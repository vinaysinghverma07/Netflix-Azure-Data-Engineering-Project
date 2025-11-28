# Databricks notebook source
    from pyspark.sql.functions import *
    from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Data Transformation

# COMMAND ----------

    df = spark.read.format("delta")\
        .option("header", True)\
        .option("inferSchema", True)\
        .load("abfss://bronze@netflixprojectdlvinay.dfs.core.windows.net/netflix_titles")

# COMMAND ----------

    df.display()


# COMMAND ----------

    # df = df.withColumn("duration_minutes")
    df = df.fillna({"duration_minutes": 0, "duration_seasons":1})

# COMMAND ----------

    df.display()

# COMMAND ----------

    df_delete_string_data = df.filter(df.duration_minutes.isin('Flying Fortress""',' and probably will.""'))
    #another way of writing above code 
    # df = df.filter(
    #     (df.duration_minutes != 'Flying Fortress""') &
    #     (df.duration_minutes != "' and probably will.\"\"\"'")
    # )

    # df.filter(df.duration_minutes.contains("""Flying Fortress"""""")).display()

    # df.filter(df.duration_minutes.contains("Flying")).display()

    df_delete_string_data.display()


# COMMAND ----------

# MAGIC %md
    # MAGIC **_Earlier we found the perfect example of lazy evaluation method of PySpark, we initially applied some transformation and then 
    # called an action, and when action was called or a job was creaed it gave us an error, for the transformation that we build earlier since 
    # there were some string operators in it and we created a transformation for converting the string to integer and it gave us an error when 
    # an action was triggered.
    # In Below commands we are replacing the dataframe, what we found earlier there were were 2 text values persisting in column name as 
    # "duration_minutes", therefore we have first identified in dataframe named as "df_delete_string_data" and removed that from the actual 
    # dataframe "df", and to add on since dataframes are immutable meaining once they are created they cannot be changed but at the same time we 
    # do have the functionality that it will replace the old data with the new one if using the same dataframe.
    # In below command ~(tilde) in this case acting as not in operator, meaning create a dataframe which does not consisting of these values

# COMMAND ----------

    df = df.filter(~df.duration_minutes.isin('Flying Fortress""',' and probably will.""'))
    df.display()

# COMMAND ----------

    df = df.withColumn("duration_minutes",col('duration_minutes').cast(IntegerType()))\
        .withColumn("duration_seasons",col('duration_seasons').cast(IntegerType()))

# COMMAND ----------

    df.printSchema()

# COMMAND ----------

    df = df.withColumn("Shorttitle",split(col('title'),':')[0])
    df.display()

# COMMAND ----------

    df = df.withColumn("rating",split(col('rating'),'-')[0])
    df.display()

# COMMAND ----------

# MAGIC ## Conditional column ##
# Creating a condition if column_Type is movie then return 0 else 1 using case when statement in spark it is called when otherwise statement

# COMMAND ----------

    df = df.withColumn("type_flag", when(col("type")=="Movie",1)\
                            .when(col("type")=="TV Show",2)\
                            .otherwise(0))
    df.display()

# COMMAND ----------

# MAGIC %md
# Using Windows function in PySpark
# MAGIC

# COMMAND ----------

    from pyspark.sql.window import Window

# COMMAND ----------

    # df = df.withColumn("duration_ranking", dense_rank().over(Window.partitionBy().orderBy(col("duration_minutes")).desc()))
    df = df.withColumn("duration_ranking", dense_rank().over(Window.orderBy(col("duration_minutes").desc())))

# COMMAND ----------

    df.display()

# COMMAND ----------

# MAGIC %md
# SQL Commands in Databricks PySpark
# Important point from PySpark SQL, if you wanted to use the sql commands in databricks pyspark first we have to use "df = spark.sql 
# ("""Select col1, col2, col3 from table """)"
# And incase if you have to use any dataframe that you created above in databricks then in that case first you have to convert it into temporary 
# view in databricks using below commands 
# 
#  Example -> df.CreateOrReplaceTempView("View_name")
#    --And then final command will looks like this
#    df = spark.sql ("""--- 3 colons are given for multi line codes
#                    Select col1, col2, col3 
#                    from View_name 
#                    """)
# Interview Question -> Can we use this same view in order to view the data into another notebook?
# Ans is No, we cannot use this same view in order to view the data into another notebook because these are temp_view and only notebook scoped 
# view meaning only specific to this particular notebook and also just like sql temp table only specific to that particular session.
# And incase if you have to use the same view if there is a requirement then in that case you have to use createOrReplaceGlobalTempView in 
# Pyspark but it will be terminated when your session are closed.
# Example -> " df.createOrReplaceGlobalTempView("global_view") 
# -- Using Select Statement
# Select * from global_temp.global_view"
# Another option in order to have cross notebook data available is creating of tables in databricks but for that you have to create first temp 
# view and then use the same temp_View for creating tables (CTAS), see example below -- 
# Step 1: Register the DataFrame as a TEMP VIEW ->
# df.createOrReplaceTempView("my_data_temp")
# -- Now SQL can access your DataFrame under the name my_data_temp.
# Step 2: Use CTAS to create a Delta table from it ->
# CREATE OR REPLACE TABLE my_table AS
# SELECT * FROM my_data_temp;

# COMMAND ----------

    df.createOrReplaceTempView("temp_view")

# COMMAND ----------

    df.createOrReplaceGlobalTempView("global_view")

# COMMAND ----------

    df = spark.sql("""
                Select * from temp_view
                """)

# COMMAND ----------

    df = spark.sql("""
                Select * from global_temp.global_view
                """)

# COMMAND ----------

    df.display()

# COMMAND ----------

### Using Aggregate Function

# COMMAND ----------

    df_vis = df.groupBy("type").agg(count("*").alias("total_count"))
    df_vis.display()

# COMMAND ----------

    df.write.format("delta")\
        .mode("overwrite")\
        .option("path","abfss://silver@netflixprojectdlvinay.dfs.core.windows.net/netflix_titles")\
        .save()

# COMMAND ----------
# Databricks notebook source
## view raw dataset

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM `hive_metastore`.`default`.`loan_001`;

# COMMAND ----------

# MAGIC %md
# MAGIC ## describe loan

# COMMAND ----------

# MAGIC
# MAGIC %sql 
# MAGIC describe loan_001

# COMMAND ----------

# MAGIC %md
# MAGIC ##load data into pyspark

# COMMAND ----------

df = spark.read.table("loan_001")
display(df)

# COMMAND ----------

df = spark.read.table("loan_001")
df_no_comment = df.drop("Comment")
display(df_no_comment)

# COMMAND ----------

# MAGIC %md
# MAGIC ## import pyspark functions

# COMMAND ----------

import pyspark.sql.functions as F
 
df = spark.read.table("loan_001")
df_no_comment = df.drop("Comment")
 
df_no_comment.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in df_no_comment.columns
]).display()

# COMMAND ----------

##remive rows wutg missing values
df = spark.read.table("loan_001")
df_no_comment = df.drop("Comment")
 
df_clean = df_no_comment.dropna(subset=["Loan"])
 
display(df_clean)

# COMMAND ----------

## verify null values have been removed
df = spark.read.table("loan_001")
df_no_comment = df.drop("Comment")
 
df_clean = df_no_comment.dropna(subset=["Loan"])
 
display(df_clean)

# COMMAND ----------

df_clean.select(
    F.count(F.when(F.col("Loan").isNull(), 1)).alias("Loan_nulls")
).display()

# COMMAND ----------

## rename columns

df = spark.read.table("loan_001")
df_no_comment = df.drop("Comment")
df_clean = df_no_comment.dropna(subset=["Loan"])
 
df_clean = df_clean.withColumnRenamed("Marital Status", "Marital_Status") \
                   .withColumnRenamed("Family Size", "Family_Size") \
                   .withColumnRenamed("Use Frequency", "Use_Frequency") \
                   .withColumnRenamed("Loan Category", "Loan_Category") \
                   .withColumnRenamed("Returned Cheque", "Returned_Cheque") \
                   .withColumnRenamed("Dishonour of Bill", "Dishonour_of_Bill")
 
display(df_clean)

# COMMAND ----------

## save the clean dataset
df_clean.write.mode("overwrite").saveAsTable("loan_analysis_clean")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM `hive_metastore`.`default`.`loan_001`;

// Databricks notebook source
spark.version

// COMMAND ----------

val myRange = spark.range(1000).toDF("number")

// COMMAND ----------

val divisBy2 = myRange.where("number % 2 = 0")

// COMMAND ----------

divisBy2.count

// COMMAND ----------

val data = spark
  .read
  .option("inferSchema", "true")
  .option("header", "true")
  .csv("/FileStore/tables/Datos_ENEMDU_PEA_v2.csv")

// COMMAND ----------

data.printSchema

// COMMAND ----------

data.count

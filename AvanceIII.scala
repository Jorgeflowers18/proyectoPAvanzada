// Databricks notebook source
import org.apache.spark.sql.types._
import sqlContext.implicits._
import org.apache.spark.sql.functions._
val Schema = StructType( Array( StructField("id_persona", DecimalType(26, 0), true), StructField("anio", IntegerType, true), StructField("mes", IntegerType, true), StructField("provincia", IntegerType, true), StructField("canton", IntegerType, true), StructField("area", StringType, true), StructField("genero", StringType, true), StructField("edad", IntegerType, true), StructField("estado_civil", StringType, true), StructField("nivel_de_instruccion", StringType, true), StructField("etnia", StringType, true), StructField("ingreso_laboral", IntegerType, true), StructField("condicion_actividad", StringType, true), StructField("sectorizacion", StringType, true), StructField("grupo_ocupacion", StringType, true), StructField("rama_actividad", StringType, true), StructField("factor_expansion", DoubleType, true) ));

// COMMAND ----------

val data = spark.read.schema(Schema).option("header", "true").option("delimiter", "\t").csv("/FileStore/tables/Datos_ENEMDU_PEA_v2-1.csv")

// COMMAND ----------

val dataUrbana = data.where($"area" === "1 - Urbana")

// COMMAND ----------

val dataRural = data.where($"area" === "2 - Rural")

// COMMAND ----------

println("Data Urbana")
dataUrbana.stat.crosstab("anio", "genero").orderBy("anio_genero").show
println("Data Rural")
dataRural.stat.crosstab("anio", "genero").orderBy("anio_genero").show

// COMMAND ----------

val dataU3 = dataUrbana.groupBy("anio").pivot("nivel_de_instruccion").agg(round(avg("ingreso_laboral"), 2)).orderBy("anio")

// COMMAND ----------

val dataU1 = dataU3.withColumn("Ninguno", $"01 - Ninguno").withColumn("Alfabetizados", $"02 - Centro de alfabetización").withColumn("Primaria", $"04 - Primaria").withColumn("Básica", $"05 - Educación Básica").withColumn("Secundaria", $"06 - Secundaria").withColumn("Media", $"07 - Educación  Media").withColumn("Tecnologías", $"08 - Superior no universitario").withColumn("Universitaria", $"09 - Superior Universitario").withColumn("Post-grado", $"10 - Post-grado")

// COMMAND ----------

display(dataU1.drop("01 - Ninguno", "02 - Centro de alfabetización", "04 - Primaria", "05 - Educación Básica", "06 - Secundaria", "07 - Educación  Media", "08 - Superior no universitario", "09 - Superior Universitario", "10 - Post-grado"))

// COMMAND ----------

display(datau1.drop("01 - Ninguno", "02 - Centro de alfabetización", "04 - Primaria", "05 - Educación Básica", "06 - Secundaria", "07 - Educación  Media", "08 - Superior no universitario", "09 - Superior Universitario", "10 - Post-grado"))

// COMMAND ----------

val dataR3 = dataRural.groupBy("anio").pivot("nivel_de_instruccion").agg(round(avg("ingreso_laboral"), 2)).orderBy("anio")
val dataR1 = dataR3.withColumn("Ninguno", $"01 - Ninguno").withColumn("Alfabetizados", $"02 - Centro de alfabetización").withColumn("Primaria", $"04 - Primaria").withColumn("Básica", $"05 - Educación Básica").withColumn("Secundaria", $"06 - Secundaria").withColumn("Media", $"07 - Educación  Media").withColumn("Tecnologías", $"08 - Superior no universitario").withColumn("Universitaria", $"09 - Superior Universitario").withColumn("Post-grado", $"10 - Post-grado")

// COMMAND ----------

display(dataR1.drop("01 - Ninguno", "02 - Centro de alfabetización", "04 - Primaria", "05 - Educación Básica", "06 - Secundaria", "07 - Educación  Media", "08 - Superior no universitario", "09 - Superior Universitario", "10 - Post-grado"))

// COMMAND ----------

display(dataR1.drop("01 - Ninguno", "02 - Centro de alfabetización", "04 - Primaria", "05 - Educación Básica", "06 - Secundaria", "07 - Educación  Media", "08 - Superior no universitario", "09 - Superior Universitario", "10 - Post-grado"))

// COMMAND ----------

val dataEtnia = data.select("etnia", "ingreso_laboral", "anio")

// COMMAND ----------

display(dataEtnia.groupBy("anio").pivot("etnia").agg(round(avg("ingreso_laboral"), 2)))

// COMMAND ----------

display(dataEtnia.groupBy("anio").pivot("etnia").agg(round(avg("ingreso_laboral"), 2)))

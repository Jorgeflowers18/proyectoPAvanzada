// Databricks notebook source
import org.apache.spark.sql.types._
import sqlContext.implicits._
import org.apache.spark.sql.functions._
val Schema = StructType( Array( StructField("id_persona", DecimalType(26, 0), true), StructField("anio", IntegerType, true), StructField("mes", IntegerType, true), StructField("provincia", IntegerType, true), StructField("canton", IntegerType, true), StructField("area", StringType, true), StructField("genero", StringType, true), StructField("edad", IntegerType, true), StructField("estado_civil", StringType, true), StructField("nivel_de_instruccion", StringType, true), StructField("etnia", StringType, true), StructField("ingreso_laboral", IntegerType, true), StructField("condicion_actividad", StringType, true), StructField("sectorizacion", StringType, true), StructField("grupo_ocupacion", StringType, true), StructField("rama_actividad", StringType, true), StructField("factor_expansion", DoubleType, true) ));

// COMMAND ----------

val data = spark.read.schema(Schema)
                .option("header", "true")
                .option("delimiter", "\t")
                .csv("/FileStore/tables/Datos_ENEMDU_PEA_v2-2.csv")

// COMMAND ----------

val dataUrbano = data.where($"area" === "1 - Urbana")
val dataRural = data.where($"area" === "2 - Rural")

// COMMAND ----------

// DBTITLE 1,¿Cuál es la población económicamente activa registrada en el área rural y en la urbana en los años 2015 al 2019?
// Área Urbana
display(dataUrbano.select("anio").groupBy("anio").count().orderBy($"anio"))

// COMMAND ----------

// Área Rural
display(dataRural.select("anio").groupBy("anio").count().orderBy($"anio"))

// COMMAND ----------

// DBTITLE 1,¿En que área se encuentra la mayor concentración de ciudadanos sin ningún tipo de instrucción?
display(data.where(($"nivel_de_instruccion" === "01 - Ninguno")).groupBy("area").count())

// COMMAND ----------

// DBTITLE 1,¿Cuál es el porcentaje de personas que registran mensualmente ingresos superiores a 1000 dólares por área?
printf("Área Urbana = %% %.2f\n", dataUrbano.select("id_persona").where($"ingreso_laboral" > 1000).count()*100/dataUrbano.count().toDouble)
printf("Área Rural = %% %.2f", dataRural.select("id_persona").where($"ingreso_laboral" > 1000).count()*100/dataRural.count().toDouble)

// COMMAND ----------

// DBTITLE 1,¿Cuál es la cantidad de personas que registran mensualmente ingresos superiores a 1000 dólares en cada nivel de instrucción?
// Área Urbana
val ingUrb1 = dataUrbano.where($"ingreso_laboral" > 1000)
val ingUrb2 = ingUrb1.groupBy("anio").pivot($"nivel_de_instruccion").count().orderBy($"anio")
val ingUrb3 = ingUrb2.withColumn("Ninguno", $"01 - Ninguno").withColumn("Alfabetizados", $"02 - Centro de alfabetización").withColumn("Primaria", $"04 - Primaria").withColumn("Básica", $"05 - Educación Básica").withColumn("Secundaria", $"06 - Secundaria").withColumn("Media", $"07 - Educación  Media").withColumn("Tecnologías", $"08 - Superior no universitario").withColumn("Universitaria", $"09 - Superior Universitario").withColumn("Post-grado", $"10 - Post-grado")

// COMMAND ----------

display(ingUrb3.drop("01 - Ninguno", "02 - Centro de alfabetización", "04 - Primaria", "05 - Educación Básica", "06 - Secundaria", "07 - Educación  Media", "08 - Superior no universitario", "09 - Superior Universitario", "10 - Post-grado"))

// COMMAND ----------

// Área Rural
val ingRur1 = dataRural.where($"ingreso_laboral" > 1000)
val ingRur2 = ingRur1.groupBy("anio").pivot($"nivel_de_instruccion").count().orderBy($"anio")
val ingRur3 = ingRur2.withColumn("Ninguno", $"01 - Ninguno").withColumn("Alfabetizados", $"02 - Centro de alfabetización").withColumn("Primaria", $"04 - Primaria").withColumn("Básica", $"05 - Educación Básica").withColumn("Secundaria", $"06 - Secundaria").withColumn("Media", $"07 - Educación  Media").withColumn("Tecnologías", $"08 - Superior no universitario").withColumn("Universitaria", $"09 - Superior Universitario").withColumn("Post-grado", $"10 - Post-grado")

// COMMAND ----------

display(ingUrb3.drop("01 - Ninguno", "02 - Centro de alfabetización", "04 - Primaria", "05 - Educación Básica", "06 - Secundaria", "07 - Educación  Media", "08 - Superior no universitario", "09 - Superior Universitario", "10 - Post-grado"))

// COMMAND ----------

// DBTITLE 1,¿Cuál es el porcentaje de ciudadanos casados que registran ingresos mayores al salario básico?
printf("Área Urbana\nAño 2015 = %% %.2f\n", dataUrbano.select("id_persona").where($"ingreso_laboral" > 400 && $"estado_civil" === "1 - Casado(a)" && $"anio" === 2015).count()*100/dataUrbano.count().toDouble)
printf("Año 2016 = %% %.2f\n", dataUrbano.select("id_persona").where($"ingreso_laboral" > 400 && $"estado_civil" === "1 - Casado(a)" && $"anio" === 2016).count()*100/dataUrbano.count().toDouble)
printf("Año 2017 = %% %.2f\n", dataUrbano.select("id_persona").where($"ingreso_laboral" > 400 && $"estado_civil" === "1 - Casado(a)" && $"anio" === 2017).count()*100/dataUrbano.count().toDouble)
printf("Año 2018 = %% %.2f\n", dataUrbano.select("id_persona").where($"ingreso_laboral" > 400 && $"estado_civil" === "1 - Casado(a)" && $"anio" === 2018).count()*100/dataUrbano.count().toDouble)
printf("Año 2019 = %% %.2f\nÁrea Rural\n", dataUrbano.select("id_persona").where($"ingreso_laboral" > 400 && $"estado_civil" === "1 - Casado(a)" && $"anio" === 2019).count()*100/dataUrbano.count().toDouble)
printf("Año 2015 = %% %.2f\n", dataRural.select("id_persona").where($"ingreso_laboral" > 400 && $"estado_civil" === "1 - Casado(a)" && $"anio" === 2015).count()*100/dataRural.count().toDouble)
printf("Año 2016 = %% %.2f\n", dataRural.select("id_persona").where($"ingreso_laboral" > 400 && $"estado_civil" === "1 - Casado(a)" && $"anio" === 2016).count()*100/dataRural.count().toDouble)
printf("Año 2017 = %% %.2f\n", dataRural.select("id_persona").where($"ingreso_laboral" > 400 && $"estado_civil" === "1 - Casado(a)" && $"anio" === 2017).count()*100/dataRural.count().toDouble)
printf("Año 2018 = %% %.2f\n", dataRural.select("id_persona").where($"ingreso_laboral" > 400 && $"estado_civil" === "1 - Casado(a)" && $"anio" === 2018).count()*100/dataRural.count().toDouble)
printf("Año 2019 = %% %.2f", dataRural.select("id_persona").where($"ingreso_laboral" > 400 && $"estado_civil" === "1 - Casado(a)" && $"anio" === 2019).count()*100/dataRural.count().toDouble)

// COMMAND ----------

// DBTITLE 1,¿Cuántas personas según el área alcanzaron un nivel de estudio superior universitario?
display(data.where(($"nivel_de_instruccion" === "08 - Superior no universitario") || ($"nivel_de_instruccion" === "09 - Superior Universitario")).groupBy("area").pivot($"nivel_de_instruccion").count().sort("area"))

// COMMAND ----------

// DBTITLE 1,¿En qué grupo de ocupación se concentra la mayor cantidad de personas sin ningún tipo de preparación?
// Área Urbana
display(sc.parallelize(Seq(
  ("Otros", 761),
  ("Trabajadores agropecuarios y pesqueros",1292),
  ("Comerciantes y servicios",1407),
  ("Ocupaciones elementales", 2129)
 )).toDF("nivel", "cant"))

// COMMAND ----------

// Área Rural
display(sc.parallelize(Seq(
  ("Otros", 1333), 
  ("Trabajadores agropecuarios y pesqueros", 10040),
  ("Ocupaciones elementales", 6016)
 )).toDF("nivel", "cant"))

// COMMAND ----------

// DBTITLE 1,¿Cuál es el grupo de ocupación predominante en cada área?
// Área Urbana
display(dataUrbano.select("grupo_ocupacion").groupBy("grupo_ocupacion").count())

// COMMAND ----------

// Área Rural
display(dataRural.select("grupo_ocupacion").groupBy("grupo_ocupacion").count())

// COMMAND ----------

// DBTITLE 1,¿Cuál es el porcentaje de personas sin ningún tipo de instrucción por cada año?
printf("Área Urbana\nAño 2015 = %% %.2f\n", (dataUrbano.where($"nivel_de_instruccion" === "01 - Ninguno" && $"anio" === 2015).count()/ dataUrbano.count().toDouble)*100)
printf("Año 2016 = %% %.2f\n", (dataUrbano.where($"nivel_de_instruccion" === "01 - Ninguno" && $"anio" === 2016).count()/ dataUrbano.count().toDouble)*100)
printf("Año 2017 = %% %.2f\n", (dataUrbano.where($"nivel_de_instruccion" === "01 - Ninguno" && $"anio" === 2017).count()/ dataUrbano.count().toDouble)*100)
printf("Año 2018 = %% %.2f\n", (dataUrbano.where($"nivel_de_instruccion" === "01 - Ninguno" && $"anio" === 2018).count()/ dataUrbano.count().toDouble)*100)
printf("Año 2019 = %% %.2f\nÁrea Rural\n", (dataUrbano.where($"nivel_de_instruccion" === "01 - Ninguno" && $"anio" === 2019).count()/ dataUrbano.count().toDouble)*100)
printf("Año 2015 = %% %.2f\n", (dataRural.where($"nivel_de_instruccion" === "01 - Ninguno" && $"anio" === 2015).count()/ dataRural.count().toDouble)*100)
printf("Año 2016 = %% %.2f\n", (dataRural.where($"nivel_de_instruccion" === "01 - Ninguno" && $"anio" === 2016).count()/ dataRural.count().toDouble)*100)
printf("Año 2017 = %% %.2f\n", (dataRural.where($"nivel_de_instruccion" === "01 - Ninguno" && $"anio" === 2017).count()/ dataRural.count().toDouble)*100)
printf("Año 2018 = %% %.2f\n", (dataRural.where($"nivel_de_instruccion" === "01 - Ninguno" && $"anio" === 2018).count()/ dataRural.count().toDouble)*100)
printf("Año 2019 = %% %.2f", (dataRural.where($"nivel_de_instruccion" === "01 - Ninguno" && $"anio" === 2019).count()/ dataRural.count().toDouble)*100)

// COMMAND ----------

// DBTITLE 1,¿Cuál es la cantidad de personas menores a 18 años en ambas áreas?. ¿Cuántas de estas pertenecen al sector informal?
val menor18Urbana = dataUrbano.select("id_persona").where($"edad" < 18).count
val menor18InformalUrbana = dataUrbano.select("id_persona").where($"edad" < 18 && $"sectorizacion" === "2 - Sector Informal").count
val promedioUrbana = (menor18InformalUrbana / dataUrbano.count.toDouble)*100

val menor18Rural = dataRural.select("id_persona").where($"edad" < 18).count
val menor18InformalRural = dataRural.select("id_persona").where($"edad" < 18 && $"sectorizacion" === "2 - Sector Informal").count
val promedioRural = (menor18InformalRural / dataRural.count.toDouble)*100

// COMMAND ----------

printf("La cantidad de personas menores de edad en el área Rural es: %d, de estos la cantidad que trabaja en el sector informal son %d lo que corresponde a un %% %.2f\nLa cantidad de personas menores de edad en el área Urbana es: %d, de estos la cantidad que trabaja en el sector informal son %d lo que corresponde a un %% %.2f\n", menor18Rural, menor18InformalRural, promedioRural, menor18Urbana, menor18InformalUrbana, promedioUrbana)

// COMMAND ----------



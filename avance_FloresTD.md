# Avance #4 de l Proyecto de Programación Avanzada (Jorge Flores)

## Sentencias para leer el archivo .csv

import org.apache.spark.sql.types._

`val Schema = StructType(
    Array(
        StructField("id_persona", DecimalType(26, 0), true), 
        StructField("anio", IntegerType, true), 
        StructField("mes", IntegerType, true), 
        StructField("provincia", IntegerType, true), 
        StructField("canton", IntegerType, true), 
        StructField("area", StringType, true), 
        StructField("genero", StringType, true), 
        StructField("edad", IntegerType, true), 
        StructField("estado_civil", StringType, true), 
        StructField("nivel_de_instruccion", StringType, true), 
        StructField("etnia", StringType, true), 
        StructField("ingreso_laboral", IntegerType, true), 
        StructField("condicion_actividad", StringType, true), 
        StructField("sectorizacion", StringType, true), 
        StructField("grupo_ocupacion", StringType, true), 
        StructField("rama_actividad", StringType, true), 
        StructField("factor_expansion", DoubleType, true)
    ));`

`val data = spark.read.schema(Schema).option("header", "true").option("delimiter", ",").csv("C:/Users/wow80/Downloads/PROGRAMACION_AVANZADA/data/Datos_ENEMDU_PEA_v2_modific.csv")` 

## División de DataFrames 

val dataUrbana = data.where($"area" === "1 - Urbana")
val dataRural = data.where($"area" === "2 - Rural")

### Conteo de cantidad de personas por género en las dos áreas

dataUrbana.stat.crosstab("anio", "genero").orderBy("anio_genero").show

dataRural.stat.crosstab("anio", "genero").orderBy("anio_genero").show

### Separación de los ingresos laborales por cada nivel de instrucción con la implementación de tablas dinámicas

## Reemplazo de nombres de columnas  para mejor vista para el usuario

val dataU2 = dataUrbana.groupBy("anio").pivot("nivel_de_instruccion").agg(round(avg("ingreso_laboral"), 2)).orderBy("anio").withColumn("Ninguno", $"01 - Ninguno").withColumn("Alfabetizados", $"02 - Centro de alfabetización").withColumn("Primaria", $"04 - Primaria").withColumn("Básica", $"05 - Educación Básica").withColumn("Secundaria", $"06 - Secundaria").withColumn("Media", $"07 - Educación  Media").withColumn("Tecnologías", $"08 - Superior no universitario").withColumn("Universitaria", $"09 - Superior Universitario").withColumn("Post-grado", $"10 - Post-grado").drop("01 - Ninguno", "02 - Centro de alfabetización", "04 - Primaria", "05 - Educación Básica", "06 - Secundaria", "07 - Educación  Media", "08 - Superior no universitario", "09 - Superior Universitario", "10 - Post-grado")

val dataR = dataRural.groupBy("anio").pivot("nivel_de_instruccion").agg(round(avg("ingreso_laboral"), 2)).orderBy("anio").withColumn("Ninguno", $"01 - Ninguno").withColumn("Alfabetizados", $"02 - Centro de alfabetización").withColumn("Primaria", $"04 - Primaria").withColumn("Básica", $"05 - Educación Básica").withColumn("Secundaria", $"06 - Secundaria").withColumn("Media", $"07 - Educación  Media").withColumn("Tecnologías", $"08 - Superior no universitario").withColumn("Universitaria", $"09 - Superior Universitario").withColumn("Post-grado", $"10 - Post-grado").drop("01 - Ninguno", "02 - Centro de alfabetización", "04 - Primaria", "05 - Educación Básica", "06 - Secundaria", "07 - Educación  Media", "08 - Superior no universitario", "09 - Superior Universitario", "10 - Post-grado")


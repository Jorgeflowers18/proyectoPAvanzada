# Ejemplo de utilización de join para fusionar dataframes

## Carga de archivo CSV

import org.apache.spark.sql.types._


#### Carga de archivo CSV principal
`val schema1 = StructType(
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

`val data = spark.read.schema(schema1).option("header", "true").option("delimiter", ",").csv("C:/Users/wow80/Downloads/PROGRAMACION_AVANZADA/data/Datos_ENEMDU_PEA_v2_modific.csv")`

#### Carga de archivo CSV con nombres y códigos de provincias
`val schema2 = StructType(
    Array(
        StructField("provincia", StringType, true), 
        StructField("nombre_prov", StringType, true)
    ));`

`val dataProv = spark.read.schema(schema2).option("header", "true").option("delimiter", ",").csv("C:/Users/wow80/Downloads/PROGRAMACION_AVANZADA/data/provincias.csv")`


## Union de archivos CSV

Usamos la función .join para poder unir dos data frames de acuerdo a una columna en específico, lo que se podría considerar como una clave comun entre ambos data frames.

`val datafinal = data.join(dataProv, "provincia")`

Ejecutando una .printSchema en datafinal podemos observar que existen 19 columnas, debido a que cuando se unieron los dataFrames se conservó la columna con los códigos de la provincia. Para poder deshacernos de esa columna utilizamos la sentencia drop.

`val df = datafinal.drop("provincia")`

Y podemos comprobar que tiene 18 columnas sin la columna provincia que contiene los códigos de estas con la sentencia printSchema.

`df.printSchema`

~~~
root
 |-- id_persona: decimal(26,0) (nullable = true)
 |-- anio: integer (nullable = true)
 |-- mes: integer (nullable = true)
 |-- canton: integer (nullable = true)
 |-- area: string (nullable = true)
 |-- genero: string (nullable = true)
 |-- edad: integer (nullable = true)
 |-- estado_civil: string (nullable = true)
 |-- nivel_de_instruccion: string (nullable = true)
 |-- etnia: string (nullable = true)
 |-- ingreso_laboral: integer (nullable = true)
 |-- condicion_actividad: string (nullable = true)
 |-- sectorizacion: string (nullable = true)
 |-- grupo_ocupacion: string (nullable = true)
 |-- rama_actividad: string (nullable = true)
 |-- factor_expansion: double (nullable = true)
 |-- nombre_prov: string (nullable = true)
~~~
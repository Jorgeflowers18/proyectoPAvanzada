# Código para excluir los outliers de la columna Ingreso Laboral y Edad del CSV


`import org.apache.spark.sql.types._`

val myDataSchema = StructType(
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
    ));

val data = spark.read.schema(myDataSchema).option("header", "true").option("delimiter", ",").csv("C:/Users/wow80/Downloads/PROGRAMACION_AVANZADA/data/Datos_ENEMDU_PEA_v2_modific.csv")

## Exlusión de nulos de la columna

data.select("Edad").where($"edad".isNotNull).count()

~~~
622776
~~~

data.select("ingreso_laboral").where($"ingreso_laboral".isNotNull).count()

~~~
504742
~~~

Como se puede ver en la columna edad no posee nulos, por lo tanto no se debe descartar ninguno. No pasa lo mismo en la columna ingreso laboral, así que se trabajarán como dos procesos independientes.

val dfEdad = data.select("Edad").where($"edad".isNotNull)
val dfIngreso = data.select("ingreso_laboral").where($"ingreso_laboral".isNotNull)

## Proceso para obtener los dataframes sin Outliers según la desviación estadística (Columna Edad)

val rangosEdad = scala.collection.mutable.ListBuffer[Long]()
val minValue = 0.0
val maxValue = 99
val bins = 5.0
val range = (maxValue - minValue) / bins
var minCounter = minValue
var maxCounter = range
val contador = 0
while (minCounter < maxValue) {
    val valoresEnUnRango = dfEdad.where($"edad".between(minCounter,maxCounter))
    rangosEdad.+=(valoresEnUnRango.count())
    minCounter = maxCounter
    maxCounter = maxCounter + range
    contador + 1
}

rangosEdad.foreach(println)

### 1. CALCULAR LA MEDIA

`val avgEdad = dfEdad.select(mean("edad")).first()(0).asInstanceOf[Double]`

### 2. CALCULAR LA DESVIACIÓN ESTÁNDAR

`val edadStdDesvEdad = dfEdad.select(stddev("edad")).first()(0).asInstanceOf[Double]`

### 3. CALCULAR LOS LIMITES

Con esto podemos obtener los límites hasta los cuales estadísticamente llegarian a ser consideradas para la presentación.

`val inferior = avg - 3 * edadStdDesvEdad`
~~~
Límite Inferior: -5.9296375603234495
~~~
`val superior = avg + 3 * edadStdDesvEdad`
~~~
Límite Superior: 87.30479973741441
~~~

### 4. FILTRAR

Entonces después de obtener los límites simplemente podemos filtrar y desechar los valore que se encuentren debajo del límite inferior y por encima del límite superior.

`val valoresMenoresLInferior = dfEdad.where($"edad" < inferior)
valoresMenoresLInferior.describe()`

`val valoresMayoresLSuperior = dfEdad.where($"edad" > superior)
valoresMayoresLSuperior.describe()`

`val dataSinOutlierStdDev = dfEdad.where($"edad" > inferior && $"edad" < superior)`

***
## Proceso para obtener los dataframes sin Outliers según la desviación estadística (Columna Ingreso Laboral)

val rangosIngreso = scala.collection.mutable.ListBuffer[Long]()
val minValue = 0.0
val maxValue = 99
val bins = 5.0
val range = (maxValue - minValue) / bins
var minCounter = minValue
var maxCounter = range
val contador = 0
while (minCounter < maxValue) {
    val valoresEnUnRango = dfIngreso.where($"ingreso_laboral".between(minCounter,maxCounter))
    rangosIngreso.+=(valoresEnUnRango.count())
    minCounter = maxCounter
    maxCounter = maxCounter + range
    contador + 1
}

rangosIngreso.foreach(println)

### 1. CALCULAR LA MEDIA

`val avgIngreso = dfIngreso.select(mean("ingreso_laboral")).first()(0).asInstanceOf[Double]`

### 2. CALCULAR LA DESVIACIÓN ESTÁNDAR

`val edadStdDesvIngreso = dfIngreso.select(stddev("ingreso_laboral")).first()(0).asInstanceOf[Double]`

### 3. CALCULAR LOS LIMITES

Con esto podemos obtener los límites hasta los cuales estadísticamente llegarian a ser consideradas para la presentación.

`val inferior = avg - 3 * edadStdDesvIngreso`
~~~
Límite Inferior: -5.9296375603234495
~~~
`val superior = avg + 3 * edadStdDesvIngreso`
~~~
Límite Superior: 87.30479973741441
~~~

### 4. FILTRAR

Entonces después de obtener los límites simplemente podemos filtrar y desechar los valore que se encuentren debajo del límite inferior y por encima del límite superior.

`val valoresMenoresLInferiorIngreso = dfIngreso.where($"ingreso_laboral" < inferior)
valoresMenoresLInferior.describe()`

`val valoresMayoresLSuperiorIngreso = dfIngreso.where($"ingreso_laboral" > superior)
valoresMayoresLSuperior.describe()`

`val dataSinOutlierStdDevIngreso = dfIngreso.where($"ingreso_laboral" > inferior && $"ingreso_laboral" < superior)`

***
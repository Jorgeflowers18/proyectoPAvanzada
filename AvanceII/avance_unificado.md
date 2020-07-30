# Avance Proyecto de Programación Avanzada
## __¿Cuál es la población economicamente activa registrada en el área rural y en la urbana en los años 2015 al 2019?__
### Urbano

`dataUrbano.select("anio").groupBy("anio").count().show()`

~~~
+----+-----+
|anio|count|
+----+-----+
|2018|71375|
|2015|78322|
|2019|71615|
|2016|66674|
|2017|84628|
+----+-----+

~~~
### Rural

`dataRural.select("anio").groupBy("anio").count().show()`

~~~
+----+-----+
|anio|count|
+----+-----+
|2018|46825|
|2015|54622|
|2019|48204|
|2016|44486|
|2017|56025|
+----+-----+
~~~
* * *

## __¿En que área, se encuentra la mayor concentración de ciudadanos sin ningún tipo de instrucción?__

### Urbano

`dataUrbano.where($"nvl_instr" === "01 - Ninguno").groupBy("nvl_instr").count().show()`

~~~
+------------+-----+
|   nvl_instr|count|
+------------+-----+
|01 - Ninguno| 5589|
+------------+-----+
~~~
### Rural

`dataRural.where($"nvl_instr" === "01 - Ninguno").groupBy("nvl_instr").count().show()`

~~~
+------------+-----+
|   nvl_instr|count|
+------------+-----+
|01 - Ninguno|17389|
+------------+-----+
~~~
* * *
## __¿Cuál es el porcentaje de personas que registran mensualmente ingresos superiores a 1000 dólares por área?__


### Urbano

`dataUrbano.select("id").where($"ingres_labo" > 1000).count()*100/dataUrbano.count().toDouble`

~~~
10.759391756616767
~~~

### Rural

`dataRural.select("id").where($"ingres_labo" > 1000).count()*100/dataRural.count().toDouble`

~~~
2.9876639937320615
~~~
* * *
## __¿Cuál es la cantidad de personas que registran mensualmente ingresos superiores a 1000 dólares en cada nivel de instrucción?__

### Urbano 
`dataUrbano.select("nvl_instr").where($"ingres_labo" > 1000).groupBy("nvl_instr").count().sort("nvl_instr").show()`
~~~
+--------------------+-----+
|           nvl_instr|count|
+--------------------+-----+
|        01 - Ninguno|   35|
|02 - Centro de al...|   10|
|       04 - Primaria| 2139|
|05 - Educación Bá...|   51|
|     06 - Secundaria| 8281|
|07 - Educación  M...|  469|
|08 - Superior no ...| 1523|
|09 - Superior Uni...|21655|
|     10 - Post-grado| 5928|
+--------------------+-----+
~~~
### Rural

`dataRural.select("nvl_instr").where($"ingres_labo" > 1000).groupBy("nvl_instr").count().sort("nvl_instr").show()`
~~~
+--------------------+-----+
|           nvl_instr|count|
+--------------------+-----+
|        01 - Ninguno|   50|
|02 - Centro de al...|   11|
|       04 - Primaria| 1593|
|05 - Educación Bá...|   42|
|     06 - Secundaria| 2321|
|07 - Educación  M...|  144|
|08 - Superior no ...|  342|
|09 - Superior Uni...| 2528|
|     10 - Post-grado|  443|
+--------------------+-----+
~~~
* * *
## __¿Cuál es el porcentaje de ciudadanos casados que registran ingresos mayores al salario basico?__

### Urbano
`dataUrbano.select("anio").where($"ingres_labo" > 400 && $"est_civil" === "1 - Casado(a)" && $"anio" === 2015).count()*100/dataUrbano.count().toDouble`
~~~
 4.465210646942949
~~~

`dataUrbano.select("anio").where($"ingres_labo" > 400 && $"est_civil" === "1 - Casado(a)" && $"anio" === 2016).count()*100/dataUrbano.count().toDouble`
~~~
3.6557939315216283
~~~

`dataUrbano.select("anio").where($"ingres_labo" > 400 && $"est_civil" === "1 - Casado(a)" && $"anio" === 2017).count()*100/dataUrbano.count().toDouble`
~~~
4.715335440965718
~~~

`dataUrbano.select("anio").where($"ingres_labo" > 400 && $"est_civil" === "1 - Casado(a)" && $"anio" === 2018).count()*100/dataUrbano.count().toDouble`
~~~
3.8380200421884307
~~~

`dataUrbano.select("anio").where($"ingres_labo" > 400 && $"est_civil" === "1 - Casado(a)" && $"anio" === 2019).count()*100/dataUrbano.count().toDouble`
~~~
3.816550102787335
~~~

### Rural
`dataRural.select("id").where($"ingres_labo" > 400 && $"est_civil" === "1 - Casado(a)" && $"anio" === 2015).count()*100/dataRural.count().toDouble`
~~~
2.3480784451675314
~~~

`dataRural.select("id").where($"ingres_labo" > 400 && $"est_civil" === "1 - Casado(a)" && $"anio" === 2016).count()*100/dataRural.count().toDouble`
~~~
1.7316778727384654
~~~

`dataRural.select("id").where($"ingres_labo" > 400 && $"est_civil" === "1 - Casado(a)" && $"anio" === 2017).count()*100/dataRural.count().toDouble`
~~~
2.2037719557726594
~~~

`dataRural.select("id").where($"ingres_labo" > 400 && $"est_civil" === "1 - Casado(a)" && $"anio" === 2018).count()*100/dataRural.count().toDouble`
~~~
1.718486420799322
~~~

`dataRural.select("id").where($"ingres_labo" > 400 && $"est_civil" === "1 - Casado(a)" && $"anio" === 2019).count()*100/dataRural.count().toDouble`
~~~
1.7096921195065597
~~~
* * *
## ¿Cuántas personas según el área alcanzaron un nivel de estudio superior?

### Rural

`dataRural.where($"nvl_instr" === "08 - Superior no universitario").groupBy("nvl_instr").count().show()`
~~~
+--------------------+-----+
|           nvl_instr|count|
+--------------------+-----+
|08 - Superior no ...| 2762|
+--------------------+-----+
~~~
`dataRural.where($"nvl_instr" === "09 - Superior Universitario").groupBy("nvl_instr").count().show()`
~~~
+--------------------+-----+
|           nvl_instr|count|
+--------------------+-----+
|09 - Superior Uni...|14700|
+--------------------+-----+
~~~

### Urbano

`dataUrbano.where($"nvl_instr" === "08 - Superior no universitario").groupBy("nvl_instr").count().show()`
~~~
+--------------------+-----+
|           nvl_instr|count|
+--------------------+-----+
|08 - Superior no ...| 9113|
+--------------------+-----+
~~~

`dataUrbano.where($"nvl_instr" === "09 - Superior Universitario").groupBy("nvl_instr").count().show()`
~~~
+--------------------+-----+
|           nvl_instr|count|
+--------------------+-----+
|09 - Superior Uni...|94925|
+--------------------+-----+
~~~
* * *
## ¿En qué grupo de ocupación se concentra la mayor cantidad de personas sin ningún tipo de preparación?

### Urbano
`dataUrbano.select("grupo_ocupacion").where($"nvl_instr" === "01 - Ninguno").groupBy("grupo_ocupacion").count().sort($"count".desc).show()`
~~~
+--------------------+-----+
|     grupo_ocupacion|count|
+--------------------+-----+
|09 - Trabajadores...| 2129|
|05 - Trabajad. de...| 1407|
|06 - Trabajad. ca...| 1292|
|07 - Oficiales op...|  492|
|                null|  120|
|08 - Operadores d...|   92|
|03 - Técnicos y p...|   27|
|04 - Empleados de...|   18|
|02 - Profesionale...|    9|
|01 - Personal dir...|    3|
+--------------------+-----+
~~~

### Rural
`dataRural.select("grupo_ocupacion").where($"nvl_instr" === "01 - Ninguno").groupBy("grupo_ocupacion").count().sort($"count".desc).show()`
~~~
+--------------------+-----+
|     grupo_ocupacion|count|
+--------------------+-----+
|06 - Trabajad. ca...|10040|
|09 - Trabajadores...| 6016|
|05 - Trabajad. de...|  614|
|07 - Oficiales op...|  503|
|08 - Operadores d...|  111|
|                null|   76|
|04 - Empleados de...|   11|
|03 - Técnicos y p...|   11|
|01 - Personal dir...|    5|
|02 - Profesionale...|    1|
|10 - Fuerzas Armadas|    1|
+--------------------+-----+
~~~
* * *
## ¿Cúal es el grupo de ocupación predominante en cada área?

### Urbano 
`dataUrbano.select("grupo_ocupacion").groupBy("grupo_ocupacion").count().sort($"count".desc).show()`
~~~
+--------------------+-----+
|     grupo_ocupacion|count|
+--------------------+-----+
|05 - Trabajad. de...|97403|
|09 - Trabajadores...|62080|
|07 - Oficiales op...|48482|
|02 - Profesionale...|41624|
|08 - Operadores d...|29877|
|03 - Técnicos y p...|23536|
|06 - Trabajad. ca...|22136|
|                null|21082|
|04 - Empleados de...|18408|
|01 - Personal dir...| 6450|
|10 - Fuerzas Armadas| 1532|
|99 - No especificado|    4|
+--------------------+-----+
~~~

### Rural
`dataRural.select("grupo_ocupacion").groupBy("grupo_ocupacion").count().sort($"count".desc).show()`
~~~
+--------------------+-----+
|     grupo_ocupacion|count|
+--------------------+-----+
|06 - Trabajad. ca...|95687|
|09 - Trabajadores...|84206|
|05 - Trabajad. de...|23749|
|07 - Oficiales op...|16909|
|08 - Operadores d...|10524|
|02 - Profesionale...| 6630|
|                null| 5179|
|03 - Técnicos y p...| 2904|
|04 - Empleados de...| 2681|
|01 - Personal dir...| 1183|
|10 - Fuerzas Armadas|  507|
|99 - No especificado|    3|
+--------------------+-----+
~~~
* * *
## ¿Cuál es el porcentaje de personas sin ningún tipo de instrucción por cada año?

### Urbano
`(dataUrbano.where($"nvl_instr" === "01 - Ninguno" && $"anio" === 2015).count()/ dataUrbano.count().toDouble)*100`
~~~
res32: Double = 0.3349310546570982
~~~
`(dataUrbano.where($"nvl_instr" === "01 - Ninguno" && $"anio" === 2016).count()/ dataUrbano.count().toDouble)*100`
~~~
res33: Double = 0.2769622182741389
~~~
`(dataUrbano.where($"nvl_instr" === "01 - Ninguno" && $"anio" === 2017).count()/ dataUrbano.count().toDouble)*100`
~~~
res34: Double = 0.33412593192955714
~~~
`(dataUrbano.where($"nvl_instr" === "01 - Ninguno" && $"anio" === 2018).count()/ dataUrbano.count().toDouble)*100`
~~~
res35: Double = 0.2769622182741389
~~~
`(dataUrbano.where($"nvl_instr" === "01 - Ninguno" && $"anio" === 2019).count()/ dataUrbano.count().toDouble)*100`
~~~
res36: Double = 0.2769622182741389
~~~

### Rural
`(dataRural.where($"nvl_instr" === "01 - Ninguno" && $"anio" === 2015).count()/ dataRural.count().toDouble)*100`
~~~
res37: Double = 1.5330066117156083
~~~
`(dataRural.where($"nvl_instr" === "01 - Ninguno" && $"anio" === 2016).count()/ dataRural.count().toDouble)*100`
~~~
res38: Double = 1.2312021809867206
~~~
`(dataRural.where($"nvl_instr" === "01 - Ninguno" && $"anio" === 2017).count()/ dataRural.count().toDouble)*100`
~~~
res39: Double = 1.4962304426731476
~~~
`(dataRural.where($"nvl_instr" === "01 - Ninguno" && $"anio" === 2018).count()/ dataRural.count().toDouble)*100`
~~~
res40: Double = 1.3307376819820755
~~~
`(dataRural.where($"nvl_instr" === "01 - Ninguno" && $"anio" === 2019).count()/ dataRural.count().toDouble)*100`
~~~
res41: Double = 1.3599187726353323
~~~
* * *
## ¿Cuál es la cantidad de personas menores a 18 años en ambas áreas?. ¿Cuántas de estas pertenecen al sector informal?

### Urbano
`val menor18Urbana = dataUrbano.select("id").where($"edad" < 18).count
val menor18InformalUrbana = dataUrbano.select("id").where($"edad" < 18 && $"sectorizacion" === "2 - Sector Informal").count
val promedioUrbana = (menor18InformalUrbana / dataUrbano.count.toDouble)*100
println("La cantidad de personas menores de edad en el área Urbana es: "+menor18Urbana+ ", de estos la cantidad que trabaja en el sector informal son "+menor18InformalUrbana +" lo que corresponde a un %"+ promedioUrbana)`

~~~
La cantidad de personas menores de edad en el área Urbana es: 4737, de estos la cantidad que trabaja en el sector informal son 2369 lo que corresponde a un %0.6357785805149565
menor18Urbana: Long = 4737
menor18InformalUrbana: Long = 2369
promedioUrbana: Double = 0.6357785805149565
~~~

### Rural
`val menor18Rural = dataRural.select("id").where($"edad" < 18).count
val menor18InformalRural = dataRural.select("id").where($"edad" < 18 && $"sectorizacion" === "2 - Sector Informal").count
val promedioRural = (menor18InformalRural / dataRural.count.toDouble)*100
println("La cantidad de personas menores de edad en el área Rural es: "+menor18Rural+ ", de estos la cantidad que trabaja en el sector informal son "+menor18InformalRural +" lo que corresponde a un %"+ promedioRural )`

~~~
La cantidad de personas menores de edad en el área Rural es: 13449, de estos la cantidad que trabaja en el sector informal son 11076 lo que corresponde a un %4.42753095993796
menor18Rural: Long = 13449
menor18InformalRural: Long = 11076
promedioRural: Double = 4.42753095993796
~~~

* * *
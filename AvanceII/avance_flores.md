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
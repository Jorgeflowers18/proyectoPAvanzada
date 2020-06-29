sentencia de divisón de los dataframes:


1. Cuál es la población economicamente activa registrada en el área rural y en la urbana en los años 2015 al 2019.

Rural
~~~
dataRural.select("anio").groupBy("anio").count().show()
~~~
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
Urbano
~~~
dataUrbano.select("anio").groupBy("anio").count().show()
~~~
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

2. En que área, se encuentra la mayor concentración de ciudadanos sin ningún tipo de instrucción.

Urbano
~~~
dataUrbano.where($"nvl_instr" === "01 - Ninguno").groupBy("nvl_instr").count().show()
~~~
~~~
+------------+-----+
|   nvl_instr|count|
+------------+-----+
|01 - Ninguno| 5589|
+------------+-----+
~~~
Rural
~~~
dataRural.where($"nvl_instr" === "01 - Ninguno").groupBy("nvl_instr").count().show()
~~~
~~~
+------------+-----+
|   nvl_instr|count|
+------------+-----+
|01 - Ninguno|17389|
+------------+-----+
~~~
3. Cuál es el porcentaje de personas que registran mensualmente ingresos superiores a 1000 dólares por área.
4. Cuál es la cantidad de personas que registran mensualmente ingresos superiores a 1000 dólares en cada nivel de instrucción.
5. Cual es el porcentaje de ciudadanos casados que registran ingresos mayores al salario basico.
6. En que grupo de ocupación se concentra la mayor cantidad de personas sin ningún tipo de preparación
7. Cúal es el grupo de ocupación predominante en cada área.
8. Cual es el porcentaje de personas sin ningún tipo de instrucción por cada año.
9. Los porcentajes de personas pertenecientes a cada etnia según la sectorización.
10. Cual es la cantidad de personas menores a 18 años en ambas áreas, y en base a esto el porcentaje de estas que pertenezcan al sector informal.


# Consultas con intérprete de SQL en Spark

### ¿Existen personas que ganen más de 30 000 dolares y solo posean una educación primaria?

`spark.sql("""
    SELECT condicion_actividad, nivel_de_instruccion, ingreso_laboral
    FROM Data
    ORDER BY 3 DESC
""")
    .where ("`nivel_de_instruccion` LIKE '04%' and `ingreso_laboral` > 30000")
    .show(false)`

~~~
+-------------------------+--------------------+---------------+
|condicion_actividad      |nivel_de_instruccion|ingreso_laboral|
+-------------------------+--------------------+---------------+
|1 - Empleo Adecuado/Pleno|04 - Primaria       |100000         |
|1 - Empleo Adecuado/Pleno|04 - Primaria       |44234          |
|1 - Empleo Adecuado/Pleno|04 - Primaria       |34330          |
+-------------------------+--------------------+---------------+
~~~

### ¿Se puede comparar la diferencia de trabajo adecuado entre hombres y género?

`spark.sql("""
    SELECT genero, count(genero), condicion_actividad
    FROM Data
    GROUP BY 1,3
""")
    .where ("condicion_actividad LIKE '1%'")
    .show(false)`

+----------+-------------+-------------------------+
|genero    |count(genero)|condicion_actividad      |
+----------+-------------+-------------------------+
|1 - Hombre|161437       |1 - Empleo Adecuado/Pleno|
|2 - Mujer |82739        |1 - Empleo Adecuado/Pleno|
+----------+-------------+-------------------------+   

### ¿Cúal es la provincia con el mayor índice de Trabajo informal, es cierto que las provincias de la costa lideran estas estadísticas?

`spark.sql("""
    SELECT nombre_prov, count(nombre_prov), sectorizacion
    FROM Data
    GROUP BY 1,3
    ORDER BY 2 DESC
""")
  .where("`sectorizacion` LIKE '2%'")
  .show(24, false)`

~~~
+------------------------------+------------------+-------------------+
|nombre_prov                   |count(nombre_prov)|sectorizacion      |
+------------------------------+------------------+-------------------+
| GUAYAS                       |31313             |2 - Sector Informal|
| MANABI                       |19463             |2 - Sector Informal|
| COTOPAXI                     |17148             |2 - Sector Informal|
| TUNGURAHUA                   |15777             |2 - Sector Informal|
| AZUAY                        |15271             |2 - Sector Informal|
| CHIMBORAZO                   |13825             |2 - Sector Informal|
| EL ORO                       |13402             |2 - Sector Informal|
| PICHINCHA                    |13116             |2 - Sector Informal|
| LOS RIOS                     |12513             |2 - Sector Informal|
| IMBABURA                     |12035             |2 - Sector Informal|
| LOJA                         |10648             |2 - Sector Informal|
| ESMERALDAS                   |10038             |2 - Sector Informal|
| BOLIVAR                      |9709              |2 - Sector Informal|
| NAPO                         |9131              |2 - Sector Informal|
| MORONA                       |8823              |2 - Sector Informal|
| ORELLANA                     |8698              |2 - Sector Informal|
| CAÑAR                        |8105              |2 - Sector Informal|
| ZAMORA                       |8010              |2 - Sector Informal|
| SANTA ELENA                  |7190              |2 - Sector Informal|
| PASTAZA                      |6854              |2 - Sector Informal|
| CARCHI                       |6589              |2 - Sector Informal|
| SUCUMBIOS                    |6480              |2 - Sector Informal|
| SANTO DOMINGO  LOS TSACHILAS |6367              |2 - Sector Informal|
| GALAPAGOS                    |684               |2 - Sector Informal|
+------------------------------+------------------+-------------------+
~~~
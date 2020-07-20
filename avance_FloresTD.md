## División de 

val areaIngreso = data.groupBy("area").pivot("nivel_de_instruccion").agg(round(avg("ingreso_laboral"), 2)).orderBy("area").show

areaIngreso.withColumn("Ninguno", $"01 - Ninguno").withColumn("Alfabetizados", $"02 - Centro de alfabetización").withColumn("Primaria", $"04 - Primaria").withColumn("Básica", $"05 - Educación Básica").withColumn("Secundaria", $"06 - Secundaria").withColumn("Media", $"07 - Educación  Media").withColumn("Tecnologías", $"08 - Superior no universitario").withColumn("Universitaria", $"09 - Superior Universitario").withColumn("Post-grado", $"10 - Post-grado").drop("01 - Ninguno", "02 - Centro de alfabetización", "04 - Primaria", "05 - Educación Básica", "06 - Secundaria", "07 - Educación  Media", "08 - Superior no universitario", "09 - Superior Universitario", "10 - Post-grado")

val anioEtnia = data.groupBy("anio", "etnia").pivot("genero").max("ingreso_laboral").orderBy("anio").show(40)

val anioEtnia 2 = anioEtnia.groupBy("anio").()


val dataUrbana = data.where($"area" === "1 - Urbana")
val dataRural = data.where($"area" === "2 - Rural")

dataUrbana.stat.crosstab("anio", "genero").orderBy("anio_genero").show

dataRural.stat.crosstab("anio", "genero").orderBy("anio_genero").show

dataUrbana.groupBy("anio").pivot("nivel_de_instruccion").agg(round(avg("ingreso_laboral"), 2)).orderBy("anio").show

dataRural.groupBy("anio").pivot("nivel_de_instruccion").agg(round(avg("ingreso_laboral"), 2)).orderBy("anio").show

dataUrbana.withColumn("Ninguno", $"01 - Ninguno").withColumn("Alfabetizados", $"02 - Centro de alfabetización").withColumn("Primaria", $"04 - Primaria").withColumn("Básica", $"05 - Educación Básica").withColumn("Secundaria", $"06 - Secundaria").withColumn("Media", $"07 - Educación  Media").withColumn("Tecnologías", $"08 - Superior no universitario").withColumn("Universitaria", $"09 - Superior Universitario").withColumn("Post-grado", $"10 - Post-grado").drop("01 - Ninguno", "02 - Centro de alfabetización", "04 - Primaria", "05 - Educación Básica", "06 - Secundaria", "07 - Educación  Media", "08 - Superior no universitario", "09 - Superior Universitario", "10 - Post-grado")

dataRural.withColumn("Ninguno", $"01 - Ninguno").withColumn("Alfabetizados", $"02 - Centro de alfabetización").withColumn("Primaria", $"04 - Primaria").withColumn("Básica", $"05 - Educación Básica").withColumn("Secundaria", $"06 - Secundaria").withColumn("Media", $"07 - Educación  Media").withColumn("Tecnologías", $"08 - Superior no universitario").withColumn("Universitaria", $"09 - Superior Universitario").withColumn("Post-grado", $"10 - Post-grado").drop("01 - Ninguno", "02 - Centro de alfabetización", "04 - Primaria", "05 - Educación Básica", "06 - Secundaria", "07 - Educación  Media", "08 - Superior no universitario", "09 - Superior Universitario", "10 - Post-grado")



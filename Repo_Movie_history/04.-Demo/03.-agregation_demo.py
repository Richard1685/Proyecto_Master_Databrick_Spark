# Databricks notebook source
# MAGIC %md
# MAGIC # Spark Agregation Functions

# COMMAND ----------

# MAGIC %md
# MAGIC  - Funciones simples  de agregacion

# COMMAND ----------

# MAGIC %run "../03_includes/configuration"

# COMMAND ----------

movie_df = spark.read.parquet(f"{silver_folder_path}/movies")

# COMMAND ----------

# importando funciones desde el API de Spark

from pyspark.sql.functions import col,count,countDistinct, sum, avg

# COMMAND ----------

# ejecutando el total de registros del dataframe movie_df
movie_df.select(count("*")).show()

# COMMAND ----------

# ejecutando por una columna especifica, del dataframe movie_df.
movie_df.select(count("year_release_date")).show()

# COMMAND ----------

 # ejecutando por una columna especifica el valor unico por registro, del dataframe movie_df.
movie_df.select(countDistinct("year_release_date")).show()

# COMMAND ----------

 # ejecutando por una columna especifica ela suma total, del dataframe movie_df.
display(movie_df.select(sum("budget")))

# COMMAND ----------

# filtro DF. por una columna un valor en duro, seleciono con 2 funciones distintas, y lo agrego a un display
# 1. Aplicar el filtro
(movie_df
    .filter("year_release_date = 2016")

# 2. Aplicar las agregaciones (Spark nombra las columnas como 'sum(budget)' y 'count(movie_id)')
    .select(sum("budget"), count("movie_id"))

# 3. Renombrar la primera columna (sum(budget))
    .withColumnRenamed("sum(budget)", "total_budget")

# 4. Renombrar la segunda columna (count(movie_id))
    .withColumnRenamed("count(movie_id)", "count_movie") 

# 5. Mostrar el resultado
    .display()
)

# COMMAND ----------

# consultando a nivel descriptivo cantidades y promedios del dataframe
display(movie_df.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC # GROUP BY

# COMMAND ----------

from pyspark.sql.functions import col,count,countDistinct, sum, avg,max,min

# COMMAND ----------

# MAGIC %md
# MAGIC  - se crear un nuevo df, del df movie_df agrupo por el campo de año.
# MAGIC  - cuando tengo mas de una funcion de agregaciones de debe considerar "agg".
# MAGIC  - dentro de "agg" considero las funciones (sum,avg,max, min,count)
# MAGIC  - adiocionamente de cada campo utlizaddo su funcion se renombre con el alias.
# MAGIC  - luego todo se mete dentro del display, que te da un rssultado en un formato tabla. 

# COMMAND ----------


movie_group_by_df = movie_df.groupBy("year_release_date") \
    .agg(
        sum("budget").alias("total_budget"),
        avg("budget").alias("avg_budget"),
        max("budget").alias("max_budget"),
        min("budget").alias("min_budget"), 
       count("movie_id").alias("count.movies"), 
    )

# COMMAND ----------

# a diferencia del caso de arriba aqui se importa con un alias y dentro del agg, se llaman a las funciones.
from pyspark.sql import functions as F

movie_df.groupBy("year_release_date").agg(
    F.sum("budget").alias("sum_budget"),
    F.max("budget").alias("max_budget")
).display() 

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # WINDON FUNCTION (Ventanas)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC  - Funcion rank = Asigna un número de orden, pero si hay un empate, las filas reciben el mismo número y se salta los siguientes números en la secuencia para compensar. ejemplo 1, luego salta hasta el 3
# MAGIC  
# MAGIC  - Funcion des_rank = Asigna un número de orden consecutivo. Si hay un empate, las filas reciben el mismo número, pero no se salta ningún número en la secuencia. ejemplo 1, luego salta al 2

# COMMAND ----------

# importando la ventana window, importando las funciones a utilizar desde el API de Spark.
from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, dense_rank


# Creando 2 df. donde particiono por 'year_release_date' y ordeno de forma desc por el campo'budget'
movie_rank = Window.partitionBy("year_release_date").orderBy(desc("budget"))
movie_dense_rank = Window.partitionBy("year_release_date").orderBy(desc("budget"))


# llamo a mi df y selecciono las columnas que quiero.
 ## filtro por el campo 'year_release_date' los no nulos
  ## agrego columnas 'rank' y 'dense_rank' y dentro de over() agrego sus variables creada en cada uno.
  
movie_df.select(
    "Title",
    "budget",
    "year_release_date"
).filter(
    "year_release_date is not null"
).withColumn(
    "rank", rank().over(movie_rank)
).withColumn(
    "dense_rank", dense_rank().over(movie_dense_rank)
).display()

# COMMAND ----------

display(movie_group_by_df)

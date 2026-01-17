# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leer todo los Datos que son requerido

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2024-12-16")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../03_includes/configuration"

# COMMAND ----------

# MAGIC %run "../03_includes/commom_functions"

# COMMAND ----------

movie_df = spark.read.parquet(f"{silver_folder_path}/movie") \
                       .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

# creando una varibale + llamando desde otro notebook a la capa silver, el archivo parquet "language"
language_df = spark.read.parquet(f"{silver_folder_path}/language")

# COMMAND ----------

# creando una varibale + llamando desde otro notebook a la capa silver, el archivo parquet "movie_language"
movie_language_df = spark.read.parquet(f"{silver_folder_path}/movie_language") \
                              .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

# creando una varibale + llamando desde otro notebook a la capa silver, el archivo parquet "genre"
genre_df = spark.read.parquet(f"{silver_folder_path}/genre")

# COMMAND ----------

# creando una varibale + llamando desde otro notebook a la capa silver, el archivo parquet "movie_genre"
movie_genre_df = spark.read.parquet(f"{silver_folder_path}/movie_genre") \
                           .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join "movie_language" y "language_df"

# COMMAND ----------

language_mov_lan_df = language_df.join(movie_language_df,
                                       language_df.language_Id == movie_language_df.language_id,
                                       "inner") \
                                       .select(language_df.language_name, movie_language_df.movie_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join "genre" y "movie_genre"

# COMMAND ----------

genre_mov_gen_def = genre_df.join(movie_genre_df,
                                   genre_df.genre_id == movie_genre_df.genre_id,
                                   "inner") \
                                   .select(genre_df.genre_name, movie_genre_df.movie_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ### Join "movie_df" , "language_mov_lan_df", "genre_mov_gen_def"

# COMMAND ----------

# MAGIC %md
# MAGIC   - Filtrar las peliculas donde su fecha de lanzamiento de peliculas sea mayor o igual al año 2000

# COMMAND ----------

movie_filter_df = movie_df.filter("year_release_date >= 2000")

# COMMAND ----------

# Join entre 3 dataframe

results_movie_genre_language = movie_filter_df.join(language_mov_lan_df,
                                    movie_filter_df.movie_id == language_mov_lan_df.movie_id,
                                    "inner") \
                                                .join(genre_mov_gen_def,
                                    movie_filter_df.movie_id == movie_genre_df.movie_id,
                                    "inner")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Agregar columna "create_date"

# COMMAND ----------

#  importo una funcion desde el API del Pyspark
from pyspark.sql.functions import lit

# COMMAND ----------

# craando una variable, selecciono los campos a traer, agrego columna.
results_df = results_movie_genre_language \
    .select("Title","duration_time","release_date","vote_average", "language_name", "genre_name") \
    .withColumn("create_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC - Ordeno por la columna "release_date" de manera descendente

# COMMAND ----------

results_order_by_df = results_df.orderBy(results_df.release_date.desc())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Escribir los datos en el Datalake - capa Golden, en formato Parquet.

# COMMAND ----------

overwrite_partition(results_order_by_df, "movie_gold", "results_movie_genre_language", "create_date") 

# COMMAND ----------

# DBTITLE 1,Create movie_gold schema
# Crear el schema si no existe
spark.sql("CREATE SCHEMA IF NOT EXISTS movie_gold")

# COMMAND ----------


# envio mi dataframe en formato parquet al contenedor de gold: 
# el write.mod= ("overwrite") sirve para cuando ya se creado el parquet y lo esta chancando y no se caiga al hacer el bucle.
# Format(tipo de archivo"Parquet") / saveAsTable =  se guarda en una BD & tabla - ADMIN por Spark.

results_order_by_df.write.mode("append").partitionBy("create_date").format("parquet").saveAsTable("movie_gold.results_movie_genre_language")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN movie_gold

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT create_date, count(1) FROM movie_gold.results_movie_genre_language
# MAGIC group by create_date;

# COMMAND ----------


# creo una variable final en formato parquet en la capa silver
##results_order_by_df.write.mode("overwrite").parquet(f"{gold_folder_path}/results_movie_genre_language")

# COMMAND ----------

display(spark.read.parquet(f"{gold_folder_path}/results_movie_genre_language"))

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP SCHEMA cast_silver CASCADE;

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC

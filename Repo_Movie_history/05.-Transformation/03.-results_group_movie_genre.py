# Databricks notebook source
dbutils.widgets.text("p_file_date", "2024-12-30")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../03_includes/configuration"

# COMMAND ----------

# MAGIC %run "../03_includes/commom_functions"

# COMMAND ----------

# creando una varibale + llamando desde otro notebook a la capa silver, el archivo parquet "movie"
  
movie_df = spark.read.parquet(f"{silver_folder_path}/movie") \
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
# MAGIC ### Join "genre" y "movie_genre"

# COMMAND ----------

genre_mov_gen_df = genre_df.join(movie_genre_df,
                                 genre_df.genre_id == movie_genre_df.genre_id,
                                 "inner") \
                            .select(genre_df.genre_name, movie_genre_df.movie_id)

# COMMAND ----------

# MAGIC %md
# MAGIC # Join "movie_df" y "genre_mov_gen_df"

# COMMAND ----------

# MAGIC %md
# MAGIC  - Filtrar peliculas donde su fecha de lanzamiento sea mayor o igual al año 2015.

# COMMAND ----------

movie_filter_df = movie_df.filter("year_release_date >= 2015")

# COMMAND ----------

results_movie_genre_df = movie_filter_df.join(genre_mov_gen_df,
                                              movie_filter_df.movie_id == genre_mov_gen_df.movie_id,
                                              "inner")

# COMMAND ----------

result_df = results_movie_genre_df.select("year_release_date", "genre_name", "budget", "renueve")

# COMMAND ----------

from pyspark.sql.functions import sum as _sum

results_group_by_df = (
    result_df
        .groupBy("year_release_date", "genre_name")
        .agg(
            _sum("budget").alias("total_budget"),
            _sum("renueve").alias("total_revenue")
        )
)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, dense_rank, rank

result_dense_rank_df = Window.partitionBy("year_release_date").orderBy(
                                                               desc("total_budget"),
                                                               desc("total_revenue"))
final_df = results_group_by_df.withColumn("dense_rank", dense_rank().over(result_dense_rank_df)) \
                              .withColumn("create_date", lit(v_file_date))



# COMMAND ----------

# MAGIC %md
# MAGIC ## Escribir datos en el Datalake en formato "Parquet" / capa Gold

# COMMAND ----------

overwrite_partition(final_df, "movie_gold", "results_group_movie_genre", "create_date") 

# COMMAND ----------

# envio mi dataframe en formato parquet al contenedor de gold: 
# el write.mod= ("overwrite") sirve para cuando ya se creado el parquet y lo esta chancando y no se caiga al hacer el bucle.
# Format(tipo de archivo"Parquet") / saveAsTable =  se guarda en una BD & tabla - ADMIN por Spark.

final_df.write.mode("append").partitionBy("create_date").format("parquet").saveAsTable("movie_gold.results_group_movie_genre")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_gold.results_group_movie_genre;

# COMMAND ----------

# MAGIC %sql
# MAGIC  SELECT 
# MAGIC  create_date, count(1)
# MAGIC  FROM movie_gold.results_group_movie_genre
# MAGIC  group by create_date;
# MAGIC

# COMMAND ----------

# creo una variable final en formato parquet en la capa gold
## --- final_df.write.mode("overwrite").parquet(f"{gold_folder_path}/results_group_movie_genre")

# COMMAND ----------

display(spark.read.parquet(f"{gold_folder_path}/results_group_movie_genre"))

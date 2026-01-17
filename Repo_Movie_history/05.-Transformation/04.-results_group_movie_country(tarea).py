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
  
country_df = spark.read.parquet(f"{silver_folder_path}/countries")

# COMMAND ----------

# creando una varibale + llamando desde otro notebook a la capa silver, el archivo parquet "movie_genre"
  
production_country_df = spark.read.parquet(f"{silver_folder_path}/production_country") \
                                  .filter(f"file_date = '{v_file_date}'")       

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join "countries" y "production_country"

# COMMAND ----------

country_mov_df = country_df.join(production_country_df,
                                 country_df.country_id == production_country_df.country_id,
                                 "inner") \
                            .select(country_df.country_name, production_country_df.movie_id)

# COMMAND ----------

# MAGIC %md
# MAGIC # Join "movie_df" y "country_mov_df"

# COMMAND ----------

# MAGIC %md
# MAGIC  - Filtrar peliculas donde su fecha de lanzamiento sea mayor o igual al año 2015.

# COMMAND ----------

movie_filter_df = movie_df.filter("year_release_date >= 2015")

# COMMAND ----------

results_movie_country_df = movie_filter_df.join(country_mov_df,
                                              movie_filter_df.movie_id == country_mov_df.movie_id,
                                              "inner")

# COMMAND ----------

display(results_movie_country_df)

# COMMAND ----------

result_df = results_movie_country_df.select("year_release_date", "budget", "renueve", "country_name")

# COMMAND ----------

from pyspark.sql.functions import sum as _sum

results_group_by_df = (
    result_df
        .groupBy("year_release_date", "country_name")
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

overwrite_partition(final_df, "movie_gold", "results_group_movie_country", "create_date") 

# COMMAND ----------

# envio mi dataframe en formato parquet al contenedor de gold: 
# el write.mod= ("overwrite") sirve para cuando ya se creado el parquet y lo esta chancando y no se caiga al hacer el bucle.
# Format(tipo de archivo"Parquet") / saveAsTable =  se guarda en una BD & tabla - ADMIN por Spark.

final_df.write.mode("append").partitionBy("create_date").format("parquet").saveAsTable("movie_gold.results_group_movie_country")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_gold.results_group_movie_country;

# COMMAND ----------

# MAGIC %sql
# MAGIC  SELECT 
# MAGIC  create_date, count(1)
# MAGIC  FROM movie_gold.results_group_movie_country
# MAGIC  group by create_date;

# COMMAND ----------

# creo una variable final en formato parquet en la capa gold
## -- final_df.write.mode("overwrite").parquet(f"{gold_folder_path}/results_group_movie_country")

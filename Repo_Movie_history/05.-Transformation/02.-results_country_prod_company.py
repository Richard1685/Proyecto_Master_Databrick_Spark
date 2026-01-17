# Databricks notebook source
dbutils.widgets.text("p_file_date", "2024-12-16")
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

# creando una varibale + llamando desde otro notebook a la capa silver, el archivo parquet "country_df"
  
country_df = spark.read.parquet(f"{silver_folder_path}/countries")

# COMMAND ----------

# creando una varibale + llamando desde otro notebook a la capa silver, el archivo parquet "production_country"
  
production_country_df = spark.read.parquet(f"{silver_folder_path}/production_country") \
                                  .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

# creando una varibale + llamando desde otro notebook a la capa silver, el archivo parquet "production_company"
  
production_company_df = spark.read.parquet(f"{silver_folder_path}/production_company") \
                                  .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

# creando una varibale + llamando desde otro notebook a la capa silver, el archivo parquet "movie_company"
  
movie_company_df = spark.read.parquet(f"{silver_folder_path}/movie_company") \
                             .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join "country" y "production_company"

# COMMAND ----------

country_prod_coun_df = country_df.join(production_country_df,
                                       country_df.country_id == production_country_df.country_id,
                                       "inner") \
                          .select(country_df.country_name, production_country_df.movie_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join "production_company" y "movie_comapny"

# COMMAND ----------

production_company_df.printSchema()

# COMMAND ----------

prod_comp_mov_comp_df = production_company_df.join(movie_company_df,
                                       production_company_df.company_id == movie_company_df.company_id,
                                       "inner") \
                          .select(production_company_df.companyName, movie_company_df.movie_id)

# COMMAND ----------

# MAGIC %md
# MAGIC # Join "movie_df" , "country_prod_coun_df" y "prod_comp_mov_comp_df"

# COMMAND ----------

# MAGIC %md
# MAGIC - filtrar las peliculas donde su fecha  de lanzamiento sea mayo o igual al año 2010

# COMMAND ----------

movie_filter_df = movie_df.filter("year_release_date >= 2010")

# COMMAND ----------

results_movie_country_prod_company_df = movie_filter_df.join(country_prod_coun_df,
                                         movie_filter_df.movie_id == country_prod_coun_df.movie_id,
                                         "inner") \
                                                        .join(prod_comp_mov_comp_df,
                                         movie_filter_df.movie_id == prod_comp_mov_comp_df.movie_id,
                                         "inner") 
                                         

# COMMAND ----------

# MAGIC %md
# MAGIC - agregar coolumna "create_date"

# COMMAND ----------

#  importo una funcion desde el API del Pyspark
from pyspark.sql.functions import lit

# COMMAND ----------

# craando una variable, selecciono los campos a traer, agrego columna.
results_df = results_movie_country_prod_company_df \
    .select("Title","budget","renueve","duration_time", "release_date","country_name","companyName") \
    .withColumn("create_date", lit(v_file_date))

# COMMAND ----------

display(results_movie_country_prod_company_df)

# COMMAND ----------

# MAGIC %md
# MAGIC - ordenar por la columna title de manera ascendente

# COMMAND ----------

results_order_by_df = results_df.orderBy(results_df.Title.asc())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Escribir los datos en el Datalake - capa Golden, en formato Parquet.

# COMMAND ----------

overwrite_partition(results_order_by_df, "movie_gold", "result_country_prod_company", "create_date") 

# COMMAND ----------

# envio mi dataframe en formato parquet al contenedor de gold: 
# el write.mod= ("overwrite") sirve para cuando ya se creado el parquet y lo esta chancando y no se caiga al hacer el bucle.
# Format(tipo de archivo"Parquet") / saveAsTable =  se guarda en una BD & tabla - ADMIN por Spark.

results_order_by_df.write.mode("append").partitionBy("create_date").format("parquet").saveAsTable("movie_gold.result_country_prod_company")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT create_date, count(1) FROM movie_gold.result_country_prod_company
# MAGIC group by create_date;

# COMMAND ----------

## results_order_by_df.write.mode("overwrite").parquet(f"{gold_folder_path}/result_country_prod_company")

# COMMAND ----------

display(spark.read.parquet(f"{gold_folder_path}/result_country_prod_company"))

# Databricks notebook source
# MAGIC %md
# MAGIC # INNER JOIN
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Como primero paso con la expresion magica (%run) llamo al path creado en otro notebook para traer su path.

# COMMAND ----------

# MAGIC %run "../03_includes/configuration"

# COMMAND ----------

# creando una varibale + llamando desde otro notebook a la capa silver, el archivo parquet "movie"
  # filtro un campo fecha del año 2007
movie_df = spark.read.parquet(f"{silver_folder_path}/movies").filter("year_release_date = 2007")

# COMMAND ----------

# creando una varibale + llamando desde otro notebook a la capa silver, el archivo parquet "production_country"
production_country_df = spark.read.parquet(f"{silver_folder_path}/production_country")

# COMMAND ----------

# creando una varibale + llamando desde otro notebook a la capa silver, el archivo parquet "countries"
countries_df = spark.read.parquet(f"{silver_folder_path}/countries")

# COMMAND ----------

# creando un variable para el join
movie_production_country_df = movie_df.join(production_country_df, # hago join con las tablas a cruzar
                             movie_df.movie_id == production_country_df.movie_id, # campos llaves de amarre
                             "inner") \
                            .select(movie_df.Title, movie_df.budget, # selecion los campos a traer.
                                        production_country_df.country_id)  

# COMMAND ----------

# creando un variable para el join / "inner") \ menciono que tipo de join estoy usando.
movie_country_df = movie_production_country_df.join(countries_df,  # hago join con las tablas a cruzar
         movie_production_country_df.country_id == countries_df.country_id,  # campos llaves de amarre
                  "inner") \
        .select(movie_production_country_df.Title, movie_production_country_df.budget,
               countries_df.country_name)  # selecion los campos a traer.

# COMMAND ----------

# MAGIC %md
# MAGIC # Outer Join

# COMMAND ----------

# MAGIC %md
# MAGIC  ## Left join

# COMMAND ----------

# creando un variable para el join
movie_production_country_df = movie_df.join(production_country_df, # hago join con las tablas a cruzar
                             movie_df.movie_id == production_country_df.movie_id, # campos llaves de amarre
                             "left") \
                            .select(movie_df.Title, movie_df.budget, # selecion los campos a traer.
                                        production_country_df.country_id)  

# COMMAND ----------

# creando un variable para el join / "left") \ menciono que tipo de join estoy usando.
movie_country_df = movie_production_country_df.join(countries_df,  # hago join con las tablas a cruzar
         movie_production_country_df.country_id == countries_df.country_id,  # campos llaves de amarre
                  "left") \
        .select(movie_production_country_df.Title, movie_production_country_df.budget,
               countries_df.country_name)  # selecion los campos a traer.

# COMMAND ----------

# MAGIC %md
# MAGIC # Rigth Outher Join

# COMMAND ----------

# creando un variable para el join
movie_production_country_df = movie_df.join(production_country_df,
                                 movie_df.movie_id == production_country_df.movie_id, 
                                 "right") \
                                 .select(movie_df.Title, movie_df.budget, 
                                         production_country_df.country_id)

# COMMAND ----------

# creando un variable para el join / "right") \ menciono que tipo de join estoy usando.
movie_country_df = movie_production_country_df.join(countries_df,  # hago join con las tablas a cruzar
         movie_production_country_df.country_id == countries_df.country_id,  # campos llaves de amarre
                  "right") \
        .select(movie_production_country_df.Title, movie_production_country_df.budget,
               countries_df.country_name)  # selecion los campos a traer.

# COMMAND ----------

# MAGIC %md
# MAGIC # Full Outher Join

# COMMAND ----------

# creando un variable para el join
movie_production_country_df = movie_df.join(production_country_df,
                                 movie_df.movie_id == production_country_df.movie_id, 
                                 "full") \
                                 .select(movie_df.Title, movie_df.budget, 
                                         production_country_df.country_id)

# COMMAND ----------

# creando un variable para el join / "full") \ menciono que tipo de join estoy usando.
movie_country_df = movie_production_country_df.join(countries_df,  # hago join con las tablas a cruzar
         movie_production_country_df.country_id == countries_df.country_id,  # campos llaves de amarre
                  "full") \
        .select(movie_production_country_df.Title, movie_production_country_df.budget,
               countries_df.country_name)  # selecion los campos a traer.

# COMMAND ----------

# MAGIC %md
# MAGIC # Semi join
# MAGIC   ## solo trae los campos del lado izquierdo

# COMMAND ----------

movie_production_country_df = movie_df.join(production_country_df, # hago join con las tablas a cruzar
                             movie_df.movie_id == production_country_df.movie_id, # campos llaves de amarre
                             "left") \
                            .select(movie_df.Title, movie_df.budget, # selecion los campos a traer.
                                        production_country_df.country_id)

# COMMAND ----------

# creando un variable para el join / "semi") \ menciono que tipo de join estoy usando.
movie_country_df = movie_production_country_df.join(countries_df,  # hago join con las tablas a cruzar
         movie_production_country_df.country_id == countries_df.country_id,  # campos llaves de amarre
                  "semi") \
        .select(movie_production_country_df.Title, movie_production_country_df.budget) # selecion los campos a traer.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Anti Join
# MAGIC   ## trae los campos nulos de la izquierda

# COMMAND ----------

movie_production_country_df = movie_df.join(production_country_df, # hago join con las tablas a cruzar
                             movie_df.movie_id == production_country_df.movie_id, # campos llaves de amarre
                             "left") \
                            .select(movie_df.Title, movie_df.budget, # selecion los campos a traer.
                                        production_country_df.country_id)

# COMMAND ----------

# creando un variable para el join / "anti") \ menciono que tipo de join estoy usando.
movie_country_df = movie_production_country_df.join(countries_df,  # hago join con las tablas a cruzar
         movie_production_country_df.country_id == countries_df.country_id,  # campos llaves de amarre
                  "anti") \
        .select(movie_production_country_df.Title, movie_production_country_df.budget) # selecion los campos a traer.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cross join
# MAGIC   ## realiza el producto cartesiano

# COMMAND ----------

movie_country_df = movie_df.crossJoin(countries_df)

# COMMAND ----------

display(movie_country_df)

# COMMAND ----------

# saco el conteo total del dataframe.
display(movie_country_df.count())

# COMMAND ----------

## Valido que la suma de ambos df. me de la misma cantida que el display lineas arriba.
int(movie_df.count()) * int(countries_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC movie
# MAGIC language
# MAGIC movie_language
# MAGIC genre
# MAGIC movie_genre
# MAGIC

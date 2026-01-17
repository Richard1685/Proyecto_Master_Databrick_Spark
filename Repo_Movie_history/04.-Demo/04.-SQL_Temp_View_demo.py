# Databricks notebook source
# MAGIC %md
# MAGIC # Acceso al Dataframe mediante SQL

# COMMAND ----------

# MAGIC %md
# MAGIC  # Local Temp View
# MAGIC   * 1.- Crear Vistas(View) temporal en la Base de datos.
# MAGIC   * 2.- Acceder a la vista (view) desde la celda "SQL".
# MAGIC   * 3.- Acceder a la vista (view) desde la celda "python"
# MAGIC   * 4.- Acceder a la vista (view) desde otro Netebook. 

# COMMAND ----------

# MAGIC %run "../03_includes/configuration"

# COMMAND ----------

# Creando un nuevo df y llamando el archivo Parquet desde la capa gold el sigte achivo.
results_movie_genre_languaje_df = spark.read.parquet(f"{gold_folder_path}/results_movie_genre_language")

# COMMAND ----------

# creo un df con un filtro del campo : duration_time menor a 50.
movie_filter_df = results_movie_genre_languaje_df.filter("duration_time < 50")

# COMMAND ----------

display(movie_filter_df)

# COMMAND ----------

# Se crea una vista temporal dentro de un DF de Spark, la opcion OrReplace es para que siempre se recree la vista una ves ejecutado.
results_movie_genre_languaje_df.createOrReplaceTempView("v_movie_genre_language")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sentencia en SQL (no te permite meterlo en una variable o DF, solo se ejcuto en el mismo notebook)
# MAGIC SELECT 
# MAGIC * 
# MAGIC FROM v_movie_genre_language 
# MAGIC WHERE duration_time < 50

# COMMAND ----------

# sentencia en python (te permite meterlo en una variable o DF,si se puede llamarlo desde otro notebook)
Results_movie_genre_language_2 = spark.sql("SELECT * FROM v_movie_genre_language WHERE duration_time < 50")


# COMMAND ----------

#           ####sentencia en python:
 # permite creando un parametro. 
p_duration_time = 50

 # considerarlo en el filtro
Results_movie_genre_language_2 = spark.sql(f"SELECT * FROM v_movie_genre_language WHERE duration_time < {p_duration_time}")

# COMMAND ----------

display(Results_movie_genre_language_2)

# COMMAND ----------

# MAGIC %md
# MAGIC # Global Temp View

# COMMAND ----------

# MAGIC %md
# MAGIC  # Objetivos:
# MAGIC   * 1.- Crear una vista(View) temporal global del Dataframe.
# MAGIC   * 2.- Acceder a la vista(View) desde la celda del "SQL".
# MAGIC   * 3.- Acceder a la vista(View) desde la celda del "PYTHON".
# MAGIC   * 4.- Acceder a la vista(View) desde otro Notebook. 

# COMMAND ----------

# Se crea una vista temporal dentro de un DF de Spark, la opcion "Global" es crear el database temp.
results_movie_genre_languaje_df.createOrReplaceGlobalTempView("gv_movie_genre_language")

# COMMAND ----------

# MAGIC %md
# MAGIC  ### consulto todas las tablas que tengo en mi base de datos global_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sentencia en SQL (debo llamar primero mi esquema, luego ya puedo llamar a mi vista tmp global.)
# MAGIC SELECT * 
# MAGIC FROM global_temp.gv_movie_genre_language

# COMMAND ----------

# sentencia en python (llamo mi esquema, luego mi tmp_globlal, para luego llamar a una table con display.)
spark.sql("SELECT * FROM global_temp.gv_movie_genre_language").display()

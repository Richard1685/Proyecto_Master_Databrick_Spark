# Databricks notebook source
# MAGIC %md
# MAGIC # SPARK FILTER TRANSFORMATION

# COMMAND ----------

# MAGIC %md
# MAGIC Como primero paso con la expresion magica (%run) llamo al path creado en otro notebook

# COMMAND ----------

# MAGIC %run "../03_includes/configuration"

# COMMAND ----------

# creando una varibale + llamando desde otro notebook a la capa silver, el archivo parquet "movie"
movie_df = spark.read.parquet(f"{silver_folder_path}/movies")

# COMMAND ----------

# MAGIC %md
# MAGIC **# Usando el filter con solo llamar al campo y su condicion por parentesis.
# MAGIC   # su and va dentro de los parentesis**

# COMMAND ----------

# Creando un variable = llamo a mi DF anterior y el filter(where), desde el campo filtro la condicion
movie_filter_df = movie_df.filter("year_release_Date = 2007")

# COMMAND ----------

# Creando un variable = llamo a mi DF anterior y el filter(where), desde el campo filtro la condicion
 # adicionalmente creo un filtro mas con el and.
movie_filter_df = movie_df.filter("year_release_Date = 2007" and "vote_average >7")

# COMMAND ----------

display(movie_filter_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **# Usando el filter con el Nombre del campo por parentesis y luego con crochetes llamar al campo y su condicion.**
# MAGIC  # su and "&" tambien por tabla, campo separado por parentesis y crochetes.

# COMMAND ----------

# Creando un variable = llamo a mi DF anterior y el filter(where).
#  desde el esquema de mi df. llamo a mi campo filtro la condicion
movie_filter_df = movie_df.filter(movie_df["year_release_Date"] == 2007)

# COMMAND ----------

movie_filter_df = movie_df.filter((movie_df["year_release_Date"] == 2007) & (movie_df["vote_average"] > 7))

# COMMAND ----------

movie_filter_df = movie_df.filter((movie_df["year_release_Date"] == 2007)

# COMMAND ----------

display(movie_filter_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **# Usando el "Where" con el Nombre del campo por parentesis y luego con crochetes llamar al campo y su condicion.**
# MAGIC  # su and "&" tambien por tabla, campo separado por parentesis y crochetes.

# COMMAND ----------

# Uso de where(), manteniendo los paréntesis
movie_filter_df = movie_df.where(
    (movie_df["year_release_Date"] == 2007)
)

# COMMAND ----------

# Uso de where(), manteniendo los paréntesis
movie_filter_df = movie_df.filter(

    (movie_df["vote_average"] > 7)
)

# COMMAND ----------

# consulto dataframe recien creado.
display(movie_filter_df)

# COMMAND ----------

# Uso de where(), manteniendo los paréntesis
movie_filter_df = movie_df.where(
    (movie_df.year_release_date == 2007) &
    (movie_df.vote_average> 7)
)

# COMMAND ----------

# consulto dataframe recien creado.
display(movie_filter_df)

# COMMAND ----------



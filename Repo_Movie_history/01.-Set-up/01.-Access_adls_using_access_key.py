# Databricks notebook source
# MAGIC %md
# MAGIC # Acceder a Azure Data Lake Storage mediante Access Key
# MAGIC
# MAGIC 1.- Establecer la configuracion de Spark "fs.azure.account.key".
# MAGIC 2.- Listar archivos del contenedor "demo".
# MAGIC 3.- Leer datos del archivo "movie.csv"

# COMMAND ----------

# devuelve el valor del texto no sea expuesto.
movie_access_key = dbutils.secrets.get(scope = "movie-history-secret-scope", key = "movie-access-key")

# COMMAND ----------

# usando el nombre de la variable del secreto
spark.conf.set(
    "fs.azure.account.key.moviehistorico.dfs.core.windows.net",
    "Oj7yvFxOTI99P/6KvQ/owLhp8dZWPRVq15cHLl/lIt9OTHVfsi7lQMrmm3bKMAPIC+s0jbn5l7EO+AStVDS04A=="
    )

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@moviehistorico.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@moviehistorico.dfs.core.windows.net/movie.csv"))

# Databricks notebook source
# MAGIC %md
# MAGIC # Acceder a Azure Data Lake Storage mediante Access Key - PDTE DE REVISAR.
# MAGIC
# MAGIC 1.- Establecer la configuracion de Spark "SAS Toquen".
# MAGIC 2.- Listar archivos del contenedor "demo".
# MAGIC 3.- Leer datos del archivo "movie.csv"

# COMMAND ----------

# devuelve el valor del texto no sea expuesto.
movie_sas_token = dbutils.secrets.get(scope = "movie-history-secret-scope", key = "movie-sas-token")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.moviehistorico.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.moviehistorico.dfs.core.windows.net", 
               "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.moviehistorico.dfs.core.windows.net", 
               movie_sas_token)

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.<moviehistorico>.dfs.core.windows.net",
    "Oj7yvFxOTI99P/6KvQ/owLhp8dZWPRVq15cHLl/lIt9OTHVfsi7lQMrmm3bKMAPIC+s0jbn5l7EO+AStVDS04A=="
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@moviehistorico.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@moviehistorico.dfs.core.windows.net/movie.csv"))

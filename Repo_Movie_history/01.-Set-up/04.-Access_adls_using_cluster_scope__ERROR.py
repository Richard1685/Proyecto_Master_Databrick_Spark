# Databricks notebook source
# MAGIC %md
# MAGIC # Acceder a Azure Data Lake Storage mediante ambito de cluster.
# MAGIC
# MAGIC # Pasos a seguir:
# MAGIC
# MAGIC 1.- Establecer la configuracion de Spark"fs.azure.account.key" en el cluster.
# MAGIC 2.- Lista de contenedor demo.
# MAGIC 3.- Leer datos del archivo "movie.cv". 

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@movie_history.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@moviehistorico.dfs.core.windows.net/movie.csv"))

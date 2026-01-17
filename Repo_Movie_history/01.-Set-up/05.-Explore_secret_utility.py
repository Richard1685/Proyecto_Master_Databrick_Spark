# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC # Explorar las capacidades de la Utilidad de "dbutils.secrets"

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

# devuelve el nombre del secreto creado.
dbutils.secrets.listScopes()

# COMMAND ----------

# devuelve el nombre del secreto dentro del key vault creado.
dbutils.secrets.list(scope = "movie-history-secret-scope")


# COMMAND ----------

# devuelve el valor del texto no sea expuesto.
dbutils.secrets.get(scope = "movie-history-secret-scope", key = "movie-access-key")

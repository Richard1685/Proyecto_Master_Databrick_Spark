# Databricks notebook source
# MAGIC %md
# MAGIC Explorar DBFS ROOT
# MAGIC
# MAGIC 1. Mostrar los directorios de los almacenamientos DBFS.
# MAGIC 2. Mostrar el contenido de un directorio DBFS dentro del root:
# MAGIC - con DBFS.
# MAGIC - sin DBFS.
# MAGIC 3. Mostrar el contenido del sistema de archivos local.
# MAGIC 4. Interactuar con el Explorador de archivos DBFS.
# MAGIC 5. Cargar el archivo al FileStore. 

# COMMAND ----------



ls dbfs:/

# COMMAND ----------

dbutils.fs.ls("/")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/databricks-datasets

# COMMAND ----------

dbutils.fs.ls("/databricks-datasets")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls file:/

# COMMAND ----------

display(dbutils.fs.ls("/FileStore"))

# COMMAND ----------

# MAGIC
# MAGIC %fs
# MAGIC ls dbfs:/

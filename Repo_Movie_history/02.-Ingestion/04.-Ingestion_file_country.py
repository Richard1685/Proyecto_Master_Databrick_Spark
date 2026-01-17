# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Ingestion del archivo "country.json"

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 0 - 
# MAGIC  # ejecutando las rutas origen y destino de los path para usar desde otro Notebook.
# MAGIC  # ejecutando las funciones para crear un campo desde otro notebook.

# COMMAND ----------

# creando una nueva variable para mi ingesta incremental.
dbutils.widgets.text("p_file_date", "2024-12-16")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

dbutils.widgets.text("p_environment", "")
v_environment = dbutils.widgets.get("p_environment")

# COMMAND ----------

# MAGIC %run "../03_includes/configuration"

# COMMAND ----------

# MAGIC %run "../03_includes/commom_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC Paso 1.- Leer archivo JSON usando "DataFrameReader" de Spark.

# COMMAND ----------

# Creando una variable con los nombre de los campos origen y su tipo de datos
countries_schema = "countryId INT, countryIsoCode STRING, countryName STRING" 

# COMMAND ----------


# creo mi df y configuro mi cabecera, esquema y la ruta del archivo origen de forma incremental
countries_df = spark.read \
               .option("multiLine",True)  \
               .schema(countries_schema) \
               .json(f"{bronze_folder_path}/{v_file_date}/country.json")

# COMMAND ----------

# creando variable con el api spark, llamo a su esquema, y el archivo de la ruta origen(json)
  # otra manera de leer archivos al de csv.
  # countries_df = spark.read \
    #            .schema(countries_schema) \
      #          .json(f"{bronze_folder_path}/country.json")

# COMMAND ----------

# ejeucto resultado de variable y sus tipo de dato de los campos.
countries_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Paso 2.- Eliminar columnas no deseadas del "DataFRame"

# COMMAND ----------

# importando la funcion columnas de una API spark.
from pyspark.sql.functions import col

# COMMAND ----------

# Creando variable, y con drop se elimina el campo que no se va considerar.
countries_dropped_df = countries_df.drop(col("countryIsoCode"))

# COMMAND ----------

# MAGIC %md
# MAGIC Paso 3.- Cambiar el nombre de las columnas y añadir los campos "ingestion_date" & "evironment"

# COMMAND ----------

# importando la funcion tipo de dtos a trabajar por una API.
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

# creando una varibale - luego importan una funcion para agregar un campo desde otro notebbok
 # con el withColumn agreggo el campo evironment 
   # la funcion Lit permite cambiar un valor a un objeto.
countries_dropped_df = add_ingestion_date(countries_dropped_df) \
                          .withColumn("environment", lit(v_environment))


# COMMAND ----------

# se crea variable usando el "withColumnRenamed" para renombrar campos. 
countries_final_df = countries_dropped_df \
                      .withColumnRenamed("countryId", "country_id") \
                      .withColumnRenamed("countryName", "country_name") \
                      .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(countries_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Paso 04.- Escribir la salida en un formato "Parquet"

# COMMAND ----------

# movie_final_df.write.mode("overwrite").format("parquet").saveAsTable("movie_silver.movie")

countries_final_df.write.mode("append").partitionBy("file_date").format("delta").saveAsTable("movie_silver.country")

# COMMAND ----------

# MAGIC %sql
# MAGIC  SELECT 
# MAGIC  file_date, count(1)
# MAGIC  FROM movie_silver.countries
# MAGIC  group by file_date;
# MAGIC

# COMMAND ----------

# transifero el df final a la ruta de la capa silver en formato parquet.
 ## --countries_final_df.write.mode("overwrite").parquet(f"{silver_folder_path}/countries")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/moviehistorico/silver/countries

# COMMAND ----------

# consulto parquet en ruta de capa silver con estructura de tabla
display(spark.read.parquet("/mnt/moviehistorico/silver/countries"))

# COMMAND ----------

dbutils.notebook.exit("Success")

# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Ingestion de archivos "movie_company_*.csv"
# MAGIC   (Archivo Multilinea)
# MAGIC   varios archivos json desde una misma carpeta de entrada.

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 0 - 
# MAGIC  # ejecutando las rutas origen y destino de los path para usar desde otro Notebook.
# MAGIC  # ejecutando las funciones para crear un campo desde otro notebook.

# COMMAND ----------

dbutils.widgets.text("p_environment", "")
v_environment = dbutils.widgets.get("p_environment")

# COMMAND ----------

# creando parametro incremenal de la fecha 2024-12-23
dbutils.widgets.text("p_file_date", "2024-12-23")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../03_includes/configuration"

# COMMAND ----------

# MAGIC %run "../03_includes/commom_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC Paso 1.- Leer archivo JSON usando "DataFrameReader" de Spark.

# COMMAND ----------

# importanndo de una API pyspark - estructuras para los tipos de campos
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

# Creando una variable con los nombre de los campos origen y su tipo de datos
movie_company_schema = StructType(fields=[
    StructField("movieId", IntegerType(), True),
    StructField("companyId", IntegerType(), True)
])

# COMMAND ----------

# creando variable con el api spark, llamo a su esquema, llamo a la ruta donde esta la carpeta con todos los    archivos a cargar.
  # si solo quisiera cargar ciertos archivos, podria poner un comodin - ejemplo:
      # .csv("/mnt/moviehistorico/bronze/production_company/production_company_*.csv")

# 1. Definimos la ruta apuntando a la CARPETA (sin .csv al final)
path_movie_company = f"{bronze_folder_path}/{v_file_date}/movie_company/"

# 2. Leemos la carpeta completa
movie_company_df = spark.read \
    .format("csv") \
    .schema(movie_company_schema) \
    .option("header", True) \
    .load(path_movie_company)

# COMMAND ----------

# ejeucto resultado de variable y ver su tabla.
display(movie_company_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Paso 2.- Cambiar el nombre de las columnas y añadir los campos "ingestion_date" & "evironment" y ademas elimnino 2 columnas.

# COMMAND ----------

# importando la funcion tipo de dtos a trabajar por una API.
from pyspark.sql.functions import current_timestamp, lit, col

# COMMAND ----------

# se inserta en la variable la funcion del path de otro notebook para crear el campo "ingestion_date" 
#  se crea campo "evironment" y columna en duro (lit) permite cambiar un valor a un objeto.
movie_company_df = add_ingestion_date(movie_company_df) \
                    .withColumn("environment", lit(v_environment)) \
                    .withColumn("file_date", lit(v_file_date)) # agregado incremental

# COMMAND ----------

 
    # se crea variable usando el "withColumnRenamed" para renombrar campos.

movie_company_final_df = movie_company_df \
    .withColumnRenamed("movieId", "movie_id") \
    .withColumnRenamed("companyId", "company_id")

                             

# COMMAND ----------

display(movie_company_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Paso 03.- Escribir la salida en un formato "Parquet"

# COMMAND ----------

# envio mi dataframe en formato parquet al contenedor de silver: 
# el write.mod= ("overwrite") sirve para cuando ya se creado el parquet y lo esta chancando y no se caiga al hacer el bucle.
# Format(tipo de archivo"Parquet") / saveAsTable =  se guarda en una BD & tabla - ADMIN por Spark.

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
# spark.sql("DROP TABLE IF EXISTS movie_silver.movie_company")

movie_company_final_df.write \
    .mode("append") \
    .partitionBy("file_date") \
    .format("parquet") \
    .saveAsTable("movie_silver.movie_company")

# COMMAND ----------

# MAGIC %sql
# MAGIC  SELECT 
# MAGIC  file_date, count(1)
# MAGIC  FROM movie_silver.movie_company
# MAGIC  group by file_date;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_silver.movie_company WHERE movie_id is not null

# COMMAND ----------

# transifero el df final a la ruta de la capa silver en formato parquet.
## -- movie_language_final_df.write.mode("overwrite").parquet(f"{silver_folder_path}/movie_language")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/moviehistorico/silver/movie_company

# COMMAND ----------

# consulto parquet en ruta de capa silver con estructura de tabla
display(spark.read.parquet("/mnt/moviehistorico/silver/movie_company"))

# COMMAND ----------

dbutils.notebook.exit("Success")

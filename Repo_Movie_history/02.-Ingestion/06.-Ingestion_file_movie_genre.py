# Databricks notebook source
# MAGIC %md
# MAGIC # Ingestion del archivo "movie_genre.json"
# MAGIC    Archivo de una linea

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
movie_genre_schema = StructType(fields=[
    StructField("movieId", IntegerType(), True),
    StructField("genreId", IntegerType(), True),
])

# COMMAND ----------

# creando variable con el api spark, llamo a su esquema, y el archivo de la ruta origen(json)
  # otra manera de leer archivos al de csv.
movie_genre_df = spark.read \
                .schema(movie_genre_schema) \
                .json(f"{bronze_folder_path}/{v_file_date}/movie_genre.json") # agregado incremental

# COMMAND ----------

# ejeucto resultado de variable y sus tipo de dato de los campos.
movie_genre_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Paso 2.- Cambiar el nombre de las columnas y añadir los campos "ingestion_date" & "evironment"

# COMMAND ----------

# importando la funcion tipo de dtos a trabajar por una API.
from pyspark.sql.functions import current_timestamp, lit, col

# COMMAND ----------

# se inserta en la variable la funcion del path de otro notebook para crear el campo "ingestion_date" 
#  se crea campo "evironment" y columna en duro (lit) permite cambiar un valor a un objeto.
movie_genre_df = add_ingestion_date(movie_genre_df) \
                .withColumn("environment", lit(v_environment)) \
                .withColumn("file_date", lit(v_file_date)) # agregado incremental

# COMMAND ----------

# se crea variable usando el "withColumnRenamed" para renombrar campos 
movie_genre_final_df = movie_genre_df \
                      .withColumnRenamed("movieId", "movie_id") \
                      .withColumnRenamed("genreId", "genre_id")

# COMMAND ----------

display(movie_genre_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Paso 03.- Escribir la salida en un formato "Parquet"

# COMMAND ----------

# overwrite_partition(movie_genre_final_df, "movie_genre_silver", "movie_genre", "file_date") 

# COMMAND ----------

# envio mi dataframe en formato parquet al contenedor de silver: 
# el write.mod= ("overwrite") sirve para cuando ya se creado el parquet y lo esta chancando y no se caiga al hacer el bucle.
# Format(tipo de archivo"Parquet") / saveAsTable =  se guarda en una BD & tabla - ADMIN por Spark.

# --carga total
  ##---movie_genre_final_df.write.mode("overwrite").format("parquet").saveAsTable("movie_silver.movie_genre")

## Carga incremental:

merge_condition = 'tgt.movie_id = src.movie_id AND tgt.genre_id = src.genre_id AND tgt.file_date = src.file_date'
merge_delta_lake(movie_genre_final_df, "movie_silver", "movie_genre", silver_folder_path, merge_condition, "file_date")





# COMMAND ----------

# MAGIC %sql
# MAGIC  SELECT 
# MAGIC  file_date, count(1)
# MAGIC  FROM movie_silver.movie_genre
# MAGIC  group by file_date;
# MAGIC

# COMMAND ----------

# transifero el df final a la ruta de la capa silver en formato parquet.
# ejemplo con partition:
 # movie_genre_final_df.write.mode("overwrite").partitionBy("movie_id").parquet(f"{silver_folder_path}/movie_genre")
 
 # sin partition
   ## --movie_genre_final_df.write.mode("overwrite").parquet(f"{silver_folder_path}/movie_genre")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/moviehistorico/silver/movie_genre

# COMMAND ----------

# consulto parquet en ruta de capa silver con estructura de tabla
display(spark.read.parquet("/mnt/moviehistorico/silver/movie_genre"))

# COMMAND ----------

dbutils.notebook.exit("Success")

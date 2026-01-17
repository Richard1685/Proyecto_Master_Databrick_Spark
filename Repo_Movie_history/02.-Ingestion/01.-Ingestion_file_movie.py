# Databricks notebook source
dbutils.widgets.text("p_environment", "")
v_environment = dbutils.widgets.get("p_environment")

# COMMAND ----------

# creando parametro incremenal de la fecha 2024-12-23
dbutils.widgets.text("p_file_date", "2024-12-23")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC
# MAGIC %run "../03_includes/configuration"

# COMMAND ----------

# MAGIC %run "../03_includes/commom_functions"

# COMMAND ----------

# verificamos que el nombre del path este asociada a la ruta del montaje.
bronze_folder_path

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 01.-Leer archivo CSV usando "DataFrameReader" de Spark

# COMMAND ----------

# sentencia donde traigo de una libreria todo los tipo de esquemas para que se consideren:

from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, DateType

# COMMAND ----------

# Luego de declarar los tipo de esquema, creo una varibale y llamo a su estructura(StructField) , los nombre de campos y el tipo de    #####  esquema a usar por cada campo y longitud:

Movie_schema = StructType( fields= [
    StructField("movieId", IntegerType(), False),
    StructField("Title", StringType(), True),
    StructField("budget", DoubleType(), True),
    StructField("homepage", StringType(), True),
    StructField("overview", StringType(), True),
    StructField("popularity", DoubleType(), True),
    StructField("yearreleaseDate", IntegerType(), True),
    StructField("ReleaseDate", DateType(), True),
    StructField("renueve", DoubleType(), True),
    StructField("durationtime", IntegerType(), True),
    StructField("tagline", StringType(), True),
    StructField("voteAverage", DoubleType(), True),
    StructField("voteAccount", IntegerType(), True)
])

# COMMAND ----------

# Vuelvo a declarar mi variable, pero ahora con el esquema creado que esta como varibale "Movie_schema"

Movie_df = spark.read  \
.option("header", True) \
.schema(Movie_schema) \
.csv (f"{bronze_folder_path}/{v_file_date}/movie.csv") # agregado incremental

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 02.- Selecionar solo columnas requeridas (diferentes maneras).

# COMMAND ----------

# 1.- Opcion de agregar columnas = creando una variable y selecciono los campos requeridos por comillas dobles separado por comas. 
Movies_select_df = Movie_df.select("movieId","Title","budget", "popularity", "yearreleaseDate", "ReleaseDate", "renueve", "voteAverage", "durationtime",  "voteAccount")


# COMMAND ----------

print(f"Total de registros: {Movies_select_df.count()}")

# COMMAND ----------

# 2.- Opcion de agregar columbas = llamar a los campos requeridos pero su esquema sera el nombre de df. origen "Movie_df." , separado        ### por comas.

Movies_select_df = Movie_df.select(Movie_df.movieId,Movie_df.Title,Movie_df.budget, Movie_df.popularity, Movie_df.yearreleaseDate, 
                     Movie_df.ReleaseDate, Movie_df.renueve, Movie_df.voteAverage, Movie_df.durationtime, Movie_df.voteAccount)

# COMMAND ----------

# reviso resultado de la nueva varibale creada.
display(Movies_select_df)

# COMMAND ----------

# 3.- Opcion de agregar columnas = llamar a los campos requeridos por su esquema que sera el nombre de df. origen "Movie_df." & '[]' luego separado por comas.

Movies_select_df = Movie_df.select(Movie_df["movieId"],Movie_df["Title"],Movie_df["budget"], Movie_df["popularity"], Movie_df["yearreleaseDate"], Movie_df["ReleaseDate"], Movie_df["renueve"], Movie_df["voteAverage"], Movie_df["durationtime"], Movie_df["voteAccount"])

# COMMAND ----------

# 4.- importo columna de una libreria

from pyspark.sql.functions import col 

# COMMAND ----------

# 4.- Opcion de agregar columnas = se importo una funcion col y se llama a los campos con su esquema 'col'

Movies_select_df = Movie_df.select(col("movieId"), col("Title"), col("budget"), col("popularity"), col("yearreleaseDate"), col("ReleaseDate"), col("renueve"), col("voteAverage"), col("durationtime"), col("voteAccount"))



# COMMAND ----------

# consideraciones:
### la opcion 1 te sirve si solo traeras las columnas y no hras otras consultas.
### las otras opciones si puedes usar funciones multiples:
### ejemplo: estoy renombrando el ultimo campo: voteAccount x vote_prueba

Movies_select_df = Movie_df.select(col("movieId"), col("Title"), col("budget"), col("popularity"), col("yearreleaseDate"), col("ReleaseDate"), col("renueve"), col("voteAverage"), col("durationtime"), col("voteAccount").alias("vote_prueba"))


# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 3.- Existen 2 maneras de cambiar columnas en un dataframe:
# MAGIC  # 1. ".withColumnRenamed"
# MAGIC  # 2. ".whitColumnsRenamed"

# COMMAND ----------

# creo una variable de la variable anterior y utilizo ".withColumnRenamed" luego hago un salto de linea \
    # entre parentesis y comillas llamo a mi campo origen separo por coma y mi campo nuevo, cierro parentesis.
    
movie_renamed_df = Movies_select_df \
                    .withColumnRenamed("movieId", "movie_id") \
                    .withColumnRenamed("yearreleaseDate", "year_release_date") \
                    .withColumnRenamed("ReleaseDate", "release_date") \
                    .withColumnRenamed("durationtime", "duration_time") \
                    .withColumnRenamed("voteAverage", "vote_average") \
                    .withColumnRenamed("vote_prueba", "vote_count")


# COMMAND ----------

# validamos el resultado con el cambio a los campos modificados.
display(movie_renamed_df)

# COMMAND ----------

# creo una variable de la variable anterior y utilizo ".withColumnsRenamed" luego hago un salto de linea \
    # entre parentesis y crochetes llamo a mi campo origen separo por coma y un espacio y mi campo nuevo, cierro crochetes y parentesis.

movie_renamed_df = Movies_select_df \
                    .withColumnsRenamed({"movieId": "movie_id", "yearreleaseDate": "year_release_date", "ReleaseDate": "release_date", "durationtime": "duration_time", "voteAverage": "vote_average", "vote_prueba": "vote_count"})

# COMMAND ----------

# validamos el resultado con el cambio a los campos modificados.
display(movie_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 4.- Agregar columnas a mi dataframe con el "whitcolumn" & "whitcolumns"

# COMMAND ----------

# importamos la funcion current_timestamp (objeto) trae fecha actual / Lit = para crear un campo que no es objeto
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

# creamos un df, llamamaos a la variable definida y su funcion add_ingestion_date campo: ("ingestion_date")
# para el campo "evironment" la funcion lit la variable creada al incio.

movie_final_df = add_ingestion_date(movie_renamed_df) \
                    .withColumn("environment", lit(v_environment)) \
                    .withColumn("file_date", lit(v_file_date)) # agregado incremental
                                               


# COMMAND ----------

# validamos el resultado con el cambio a los campos modificados.
display(movie_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 05.- Escribir datos en datalake en formato "Parquet" - silver

# COMMAND ----------

# envio mi dataframe en formato parquet al contenedor de silver: 
# la ruta del montaje la modifico por un path creado como variable, interpolamos antes con la f{}
# el mode("overwrite") sirve para cuando ya se creado el parqueT y lo esta chancando y no se caiga al hacer el bucle.

## -----movie_final_df.write.mode("overwrite").parquet(f"{silver_folder_path}/movies")

# COMMAND ----------

# Creamos una varibale con la ruta del parquet creado.
 ## --- df = spark.read.parquet(f"{silver_folder_path}/movies")

# COMMAND ----------

#overwrite_partition(movie_final_df, "movie_silver", "movie", "file_date") 

# COMMAND ----------

# envio mi dataframe en formato parquet al contenedor de silver: 
# el write.mod= ("overwrite") sirve para cuando ya se creado el parquet y lo esta chancando y no se caiga al hacer el bucle.
# Format(tipo de archivo"Parquet") / saveAsTable =  se guarda en una BD & tabla - ADMIN por Spark.

# movie_final_df.write.mode("overwrite").format("parquet").saveAsTable("movie_silver.movie")

# COMMAND ----------

# MAGIC %md
# MAGIC * se crean variables para el merge

# COMMAND ----------

merge_condition = 'tgt.movie_id = src.movie_id AND tgt.file_date = src.file_date'
merge_delta_lake(movie_final_df, "movie_silver", "movie", silver_folder_path, merge_condition, "file_date")

# COMMAND ----------

# MAGIC %sql
# MAGIC  SELECT 
# MAGIC  file_date, count(1)
# MAGIC  FROM movie_silver.movie
# MAGIC  group by file_date;

# COMMAND ----------

dbutils.notebook.exit("Success")

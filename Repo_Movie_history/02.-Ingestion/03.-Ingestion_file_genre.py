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

genre_schema = StructType( fields= [
     StructField("genreId", IntegerType(),False),
     StructField("genreName", StringType(),True)
])

# COMMAND ----------

# creo mi df y configuro mi cabecera, esquema y la ruta del archivo origen de forma incremental
genre_df = spark.read \
               .option("header", True) \
               .schema(genre_schema) \
               .csv(f"{bronze_folder_path}/{v_file_date}/genre.csv")


# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 02.- Selecionar solo columnas requeridas (diferentes maneras).

# COMMAND ----------

# importando la funcion columnas a trabajar por una API.
from pyspark.sql.functions import col

# COMMAND ----------

# 1.- Opcion de agregar columnas = creando una variable y selecciono los campos requeridos por comillas dobles separado por comas. 
genre_select_df = genre_df.select(col("genreId"), col("genreName"))


# COMMAND ----------

# reviso resultado de la nueva varibale creada.
display(genre_select_df)

# COMMAND ----------

# Creando un DF, luego con el whitcolumnaRenamed modifico los nombre de las columnas.
genre_name_df = genre_select_df.withColumnRenamed("genreId", "genre_id").withColumnRenamed("genreName", "genre_name")

# COMMAND ----------

# reviso resultado de la nueva varibale creada.
display(genre_name_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 4.- Agregar columnas a mi dataframe con el "whitcolumn" & "whitcolumns"

# COMMAND ----------

# importando la funcion columnas de una para current(fecha actual) y lit(convertir  valor por un objeto).
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

# creamos un df, llamamaos a la variable definida y su funcion add_ingestion_date campo: ("ingestion_date")
# para el campo "evironment" la funcion lit la variable creada al incio.

genre_df = add_ingestion_date(genre_name_df) \
                                .withColumn("environment", lit(v_environment)) \
                                .withColumn("file_date", lit(v_file_date))
                                               




# COMMAND ----------

# validamos el resultado con el cambio a los campos modificados.
display(genre_df)

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

# envio mi dataframe en formato parquet al contenedor de silver: 
# el write.mod= ("overwrite") sirve para cuando ya se creado el parquet y lo esta chancando y no se caiga al hacer el bucle.
# Format(tipo de archivo"Parquet") / saveAsTable =  se guarda en una BD & tabla - ADMIN por Spark.

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
#spark.sql("DROP TABLE IF EXISTS movie_silver.genre_df")

genre_df.write \
    .mode("append") \
    .partitionBy("file_date") \
    .format("delta") \
    .saveAsTable("movie_silver.genre")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN movie_silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC  SELECT 
# MAGIC  file_date, count(1)
# MAGIC  FROM movie_silver.genre
# MAGIC  group by file_date;

# COMMAND ----------

dbutils.notebook.exit("Success")

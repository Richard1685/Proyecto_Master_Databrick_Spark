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

Language_schema = StructType( fields= [
     StructField("languageId", IntegerType(),False),
     StructField("languageCode", StringType(),True),
     StructField("languageName", StringType(),True)
])

# COMMAND ----------

# creo mi df y configuro mi cabecera, esquema y la ruta del archivo origen de forma incremental
Language_df = spark.read \
               .option("header", True) \
               .schema(Language_schema) \
               .csv(f"{bronze_folder_path}/{v_file_date}/language.csv")


# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 02.- Selecionar solo columnas requeridas (diferentes maneras).

# COMMAND ----------

# importando la funcion columnas a trabajar por una API.
from pyspark.sql.functions import col

# COMMAND ----------

# 1.- Opcion de agregar columnas = creando una variable y selecciono los campos requeridos por comillas dobles separado por comas. 
Language_select_df = Language_df.select(col("languageId"), col("languageName"))


# COMMAND ----------

# reviso resultado de la nueva varibale creada.
display(Language_select_df)

# COMMAND ----------

# Creando un DF, luego con el whitcolumnaRenamed modifico los nombre de las columnas.
language_name_df = Language_select_df.withColumnRenamed("languageId", "language_Id").withColumnRenamed("languageName", "language_name")

# COMMAND ----------

# reviso resultado de la nueva varibale creada.
display(language_name_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 4.- Agregar columnas a mi dataframe con el "whitcolumn" & "whitcolumns"

# COMMAND ----------

# importando la funcion columnas de una para current(fecha actual) y lit(convertir  valor por un objeto).
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

# creamos un df, llamamaos a la variable definida y su funcion add_ingestion_date campo: ("ingestion_date")
# para el campo "evironment" la funcion lit la variable creada al incio.

Language_final_df = add_ingestion_date(language_name_df) \
                                .withColumn("environment", lit(v_environment)) \
                                .withColumn("file_date", lit(v_file_date))
                                               


# COMMAND ----------

# validamos el resultado con el cambio a los campos modificados.
display(Language_final_df)

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

overwrite_partition(Language_final_df, "movie_silver", "language", "file_date") 

# COMMAND ----------

# envio mi dataframe en formato parquet al contenedor de silver: 
# el write.mod= ("overwrite") sirve para cuando ya se creado el parquet y lo esta chancando y no se caiga al hacer el bucle.
# Format(tipo de archivo"Parquet") / saveAsTable =  se guarda en una BD & tabla - ADMIN por Spark.

# movie_final_df.write.mode("overwrite").format("parquet").saveAsTable("movie_silver.movie")

Language_final_df.write.mode("append").partitionBy("file_date").format("delta").saveAsTable("movie_silver.Language")

# COMMAND ----------

# MAGIC %sql
# MAGIC  SELECT 
# MAGIC  file_date, count(1)
# MAGIC  FROM movie_silver.Language
# MAGIC  group by file_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_silver.Language

# COMMAND ----------

dbutils.notebook.exit("Success")

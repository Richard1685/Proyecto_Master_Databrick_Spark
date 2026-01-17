# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Ingestion del archivo "languaje_role.json"

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

# sentencia donde traigo de una libreria todo los tipo de esquemas para que se consideren:

from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, DateType

# COMMAND ----------

# Creando una variable con los nombre de los campos origen y su tipo de datos
language_role_schema = StructType(fields=[
    StructField("roleId", IntegerType(), True),
    StructField("languageRole", StringType(), True),
    StructField("ingestion_date", DateType(), True)
])

# COMMAND ----------


# creo mi df y configuro mi cabecera, esquema y la ruta del archivo origen de forma incremental
languaje_role_df = spark.read \
               .option("multiLine",True)  \
               .schema(language_role_schema) \
               .json(f"{bronze_folder_path}/{v_file_date}/language_role.json")

# COMMAND ----------

# creando variable con el api spark, llamo a su esquema, y el archivo de la ruta origen(json)
  # otra manera de leer archivos al de csv.
  # countries_df = spark.read \
    #            .schema(countries_schema) \
      #          .json(f"{bronze_folder_path}/country.json")

# COMMAND ----------

# ejeucto resultado de variable y sus tipo de dato de los campos.
languaje_role_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Paso 2.- Eliminar columnas no deseadas del "DataFRame"

# COMMAND ----------

# importando la funcion columnas de una API spark.
from pyspark.sql.functions import col, lit

# COMMAND ----------

# Creando variable, y con drop se elimina el campo que no se va considerar.
language_role_final_df = languaje_role_df \
    .withColumnRenamed("roleId", "role_id") \
    .withColumnRenamed("languageRole", "language_role") \
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC Paso 3.- Cambiar el nombre de las columnas y añadir los campos "ingestion_date" & "evironment"

# COMMAND ----------

# importando la funcion tipo de dtos a trabajar por una API.
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------


# se crea variable usando el "withColumnRenamed" para renombrar campos. 
language_role_df = add_ingestion_date(language_role_final_df) \
                  .withColumn("enviroment", lit("PROD")) \
                  .withColumn("file_date", lit(v_file_date))


# COMMAND ----------

display(language_role_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Paso 04.- Escribir la salida en un formato "Parquet"

# COMMAND ----------

 #overwrite_partition(language_role_df, "language_silver", "language_role", "file_date") 

# COMMAND ----------

##language_role_df.write.mode("append").partitionBy("file_date").format("parquet").saveAsTable("movie_silver.#language_role")

# COMMAND ----------

# envio mi dataframe en formato parquet al contenedor de silver: 
# el write.mod= ("overwrite") sirve para cuando ya se creado el parquet y lo esta chancando y no se caiga al hacer el bucle.
#Format(tipo de archivo"Parquet") / saveAsTable =  se guarda en una BD & tabla - ADMIN por Spark.

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
#spark.sql("DROP TABLE IF EXISTS movie_silver.language_role")

language_role_df.write \
.mode("append") \
.partitionBy("file_date") \
.format("delta") \
.saveAsTable("movie_silver.language_role")

# COMMAND ----------

# MAGIC %sql
# MAGIC  SELECT 
# MAGIC  file_date, count(1)
# MAGIC  FROM movie_silver.language_role
# MAGIC  group by file_date;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN movie_silver;
# MAGIC

# COMMAND ----------

# transifero el df final a la ruta de la capa silver en formato parquet.
 ## --countries_final_df.write.mode("overwrite").parquet(f"{silver_folder_path}/countries")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/moviehistorico/silver/language_role

# COMMAND ----------

# consulto parquet en ruta de capa silver con estructura de tabla
display(spark.read.parquet("/mnt/moviehistorico/silver/language_role"))

# COMMAND ----------

dbutils.notebook.exit("Success")

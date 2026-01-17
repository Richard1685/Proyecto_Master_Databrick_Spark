# Databricks notebook source
# MAGIC %md
# MAGIC # Ingestion del archivo "person.json"
# MAGIC
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

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

name_schema = StructType(fields=[
    StructField("forename", StringType(), True),
    StructField("surname", StringType(),True)
])

# COMMAND ----------

persons_schema = StructType(fields=[
    StructField("personId", IntegerType(), False),
    StructField("personName", name_schema)
])

# COMMAND ----------

persons_df = spark.read \
            .schema(persons_schema) \
            .json (f"{bronze_folder_path}/{v_file_date}/person.json") # agregado incremental            

# COMMAND ----------

persons_df.printSchema()

# COMMAND ----------

display(persons_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Paso 2.- Renonbrar las columnas y añadir nuevas columnas:
# MAGIC
# MAGIC 1- "personId" renombrar a "person_id".
# MAGIC 2- Agregar las columnas "ingestion_date" y "evironment".
# MAGIC 3- Agregar la columnas "name" a partir de la concatenacion de "forename" y "sumame".

# COMMAND ----------

# importando la funcion columnas de una API spark.
from pyspark.sql.functions import col, concat, current_timestamp, lit;

# COMMAND ----------

# se inserta en la variable la funcion del path de otro notebook para crear el campo "ingestion_date" 
#  se crea campo "evironment" y columna en duro (lit)
persons_df = add_ingestion_date(persons_df) \
                .withColumn("environment", lit(v_environment)) \
                .withColumn("file_date", lit(v_file_date)) # agregado incremental


# COMMAND ----------

# Creando variable para renombrar columna, 
# se concatena otros columna ("name" - quitando(forename + surname) y slo quedarme con los nombres ).
persons_with_columns_df = persons_df \
                            .withColumnRenamed("personsId", "persons_id") \
                            .withColumn("name",
                                    concat(
                                        col("personName.forename"),
                                        lit(" "),
                                        col("personName.surname")
                                             )
                                           )

# COMMAND ----------

# consultando ejecucion con los cambios modificados
display(persons_with_columns_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Paso 3.- Eliminar columna no requerida "personsName"

# COMMAND ----------

# Elimino columna usando la funcion drop(col)
persons_final_df = persons_with_columns_df.drop(col("personName"))

# COMMAND ----------

display(persons_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Paso 04.- Escribir la salida en un formato "Parquet"

# COMMAND ----------

# overwrite_partition(persons_final_df, "persons_silver", "persons", "file_date") 

# COMMAND ----------

# envio mi dataframe en formato parquet al contenedor de silver: 
# el write.mod= ("overwrite") sirve para cuando ya se creado el parquet y lo esta chancando y no se caiga al hacer el bucle.
# Format(tipo de archivo"Parquet") / saveAsTable =  se guarda en una BD & tabla - ADMIN por Spark.

## Carga total:
#  persons_final_df.write.mode("overwrite").format("parquet").saveAsTable("movie_silver.persons")


## incremental por merge en delta:

merge_condition = 'tgt.personid = src.personid AND tgt.file_date = src.file_date'
merge_delta_lake(persons_final_df, "movie_silver", "persons", silver_folder_path, merge_condition, "file_date")


# COMMAND ----------

# MAGIC %sql
# MAGIC  SELECT 
# MAGIC  file_date, count(1)
# MAGIC  FROM movie_silver.persons
# MAGIC  group by file_date;

# COMMAND ----------

# transifero el df final a la ruta de la capa silver en formato parquet.
# ---persons_final_df.write.mode("overwrite").parquet(f"{silver_folder_path}/persons")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/moviehistorico/silver/persons

# COMMAND ----------

# consulto parquet en ruta de capa silver con estructura de tabla
display(spark.read.parquet("/mnt/moviehistorico/silver/persons"))

# COMMAND ----------

dbutils.notebook.exit("Success")

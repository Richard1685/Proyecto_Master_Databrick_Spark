# Databricks notebook source
# MAGIC %md
# MAGIC # se crea un funcion atraves de un dataframe: 
# MAGIC  #add_ingestion_date 
# MAGIC   #columna a crear: ingestion_date.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df

# COMMAND ----------

## Creacion de funcion para sobrescribir particiones duplicadas y cada vez que ejecute una fecha no se duplique sino solo traiga lo aun no cargado.

def overwrite_partition(input_df, db_name, table_name, column_partition):
    for item_list in input_df.select(f"{column_partition}").distinct().collect():
       if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
           spark.sql(f"ALTER TABLE {db_name}.{table_name} DROP IF EXISTS PARTITION ({column_partition} = '{item_list[column_partition]}')")

# COMMAND ----------

## modificando el tipo de carga incremental, ahora en formato delta con MERGE y quitando duplicados con la funcion if. 
    ## en sentencia python, adicionalmente se agrega una funcion en python.

def merge_delta_lake(input_df, db_name, table_name, folder_path, merge_condition, partition_column):
  
  if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):

    deltaTable = DeltaTable.forPath(spark, f'{folder_path}/{table_name}')
    deltaTable.alias('tgt') \
    .merge(
      input_df.alias('src'),
      merge_condition
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

  else: 
    input_df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------



# Databricks notebook source
# MAGIC %md
# MAGIC # Read & Write en Delta Lake.
# MAGIC   * 1- Escribir datos en Delta Lake (Managed Table).
# MAGIC   * 2- Escribir datos en Delta Lake (External Table).
# MAGIC   * 3- Leer datos desde Delta Lake (Table).
# MAGIC   * 4- Leer datos desde Delta Lake (File).
# MAGIC      

# COMMAND ----------



# COMMAND ----------

# ---crecion de BD "MOVIE_DEMO" en la ruta de location
%sql
CREATE SCHEMA IF NOT EXISTS movie_demo
LOCATION "/mnt/moviehistorico/demo"

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

# creando un dataframe movie_df / configuraciones(api, cabecera, esquema, ruta y tipo de archivo)
movie_df = spark.read \
            .option("header", True) \
            .schema(Movie_schema) \
            .csv("/mnt/moviehistorico/bronze/2024-12-30/movie.csv")


# COMMAND ----------

# Correcto: Primero limitas los datos, luego los muestras
display(movie_df.limit(10))

# COMMAND ----------

# Creando un tabla en formato "delta" en la BD movie_demo (admin. databrick)
movie_df.write.format("delta").mode("overwrite").saveAsTable("movie_demo.movie_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_demo.movie_managed limit 10;

# COMMAND ----------

# MAGIC %md
# MAGIC * Creando un tabla en formato "delta" en la BD movie (externa)

# COMMAND ----------


movie_df.write.format("delta").mode("overwrite").save("/mnt/moviehistorico/demo/movie_external")

# COMMAND ----------

# MAGIC %md
# MAGIC * Creando Table movier_external "DELTA" en el contenedor demo.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE movie_demo.movie_external
# MAGIC USING DELTA
# MAGIC LOCATION "/mnt/moviehistorico/demo/movie_external"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_demo.movie_external LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC * Creacion de Dataframe en "delta" en contenedor demo. 

# COMMAND ----------

movie_external_df = spark.read.format("delta").load("/mnt/moviehistorico/demo/movie_external")


# COMMAND ----------

display(movie_external_df)

# COMMAND ----------

display(movie_df)

# COMMAND ----------

# MAGIC %md
# MAGIC * El Dataframe creado , se va particionar por el campo fecha.

# COMMAND ----------

movie_df.write.format("delta").mode("overwrite").partitionBy("yearreleaseDate").saveAsTable("movie_demo.movie_partitioned")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS movie_demo.movie_partitioned; 

# COMMAND ----------

# MAGIC %md
# MAGIC # UPDATE & DELETE EN DELTA LAKE
# MAGIC  * 1.- Update desde delta Lake.
# MAGIC  * 2.- Delete desde delta Lake. 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_demo.movie_managed limit 10;

# COMMAND ----------

# MAGIC %md
# MAGIC # Actualizando tabla movie_managed + Campo durationtime con comando SPARK-SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE movie_demo.movie_managed
# MAGIC SET durationtime = 60
# MAGIC WHERE yearreleaseDate = 2012; 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_demo.movie_managed where yearreleaseDate = 2012

# COMMAND ----------

# MAGIC %md
# MAGIC * haciendo una actualizacion en sentencia PYTHON./ Creando un DF. deltaTable

# COMMAND ----------

from delta.tables import *

deltaTable = DeltaTable.forPath(spark, '/mnt/moviehistorico/demo/movie_managed')

# declare the predicate by using a SQL- Formatted String.
deltaTable.update (
  condition = "yearreleaseDate = 2013",
  set = { "durationtime": "100" }
)


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_demo.movie_managed where yearreleaseDate = 2013

# COMMAND ----------

# MAGIC %md
# MAGIC * Eliminando registros con comandoSpark Sql

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM movie_demo.movie_managed
# MAGIC where yearreleaseDate = 2014; 

# COMMAND ----------

# MAGIC %md
# MAGIC * Elimnando regsitros mediante lenguage PYTHON.

# COMMAND ----------

from delta.tables import *

deltaTable = DeltaTable.forPath(spark, '/mnt/moviehistorico/demo/movie_managed')

# declare the predicate by using a SQL- Formatted String.
deltaTable.delete ( "yearreleaseDate = 2015")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_demo.movie_managed WHERE yearreleaseDate = 2015;

# COMMAND ----------

# MAGIC %md
# MAGIC # MERGE /UPSERT en Delta Lake

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

# creando un dataframe movie_df / configuraciones(api, cabecera, esquema, ruta y tipo de archivo)
                  # / filtro por el campo de fecha + selecciono los campos que usare.
movie_day1_df = spark.read \
              .option("header", True) \
              .schema(Movie_schema) \
              .csv("/mnt/moviehistorico/bronze/2024-12-30/movie.csv") \
              .filter("yearreleaseDate < 2000") \
              .select("movieId", "Title", "yearreleaseDate", "ReleaseDate", "durationtime")

# COMMAND ----------

display(movie_day1_df)

# COMMAND ----------

# Creando una vista temp. de movei_day1
movie_day1_df.createOrReplaceTempView("movie_day1")

# COMMAND ----------

# creando un dataframe movie_df / configuraciones(api, cabecera, esquema, ruta y tipo de archivo)
                  # / filtro  entre rango de fecha +
                  #  selecciono los campos que usare y pongo el campo title en mayuscula.

from pyspark.sql.functions import upper

movie_day2_df = spark.read \
              .option("header", True) \
              .schema(Movie_schema) \
              .csv("/mnt/moviehistorico/bronze/2024-12-30/movie.csv") \
              .filter("yearreleaseDate BETWEEN 1998 and 2005") \
              .select("movieId", upper("Title").alias("title"), "yearreleaseDate", "ReleaseDate", "durationtime")

# COMMAND ----------

display(movie_day2_df)

# COMMAND ----------

# Creando una vista temp. de movei_day2
movie_day2_df.createOrReplaceTempView("movie_day2")

# COMMAND ----------

# creando un dataframe movie_df / configuraciones(api, cabecera, esquema, ruta y tipo de archivo)
                  # / filtro  entre rango de fecha Y/O otro rango de fecha+
                  #  selecciono los campos que usare y pongo el campo title en mayuscula.

from pyspark.sql.functions import upper

movie_day3_df = spark.read \
              .option("header", True) \
              .schema(Movie_schema) \
              .csv("/mnt/moviehistorico/bronze/2024-12-30/movie.csv") \
              .filter("yearreleaseDate BETWEEN 1983 and 1998 OR yearreleaseDate BETWEEN 2006 and 2010") \
              .select("movieId", upper("Title").alias("title"), "yearreleaseDate", "ReleaseDate", "durationtime")

# COMMAND ----------

describe(movie_day3_df)

# COMMAND ----------

# MAGIC %md
# MAGIC * # Creando tabla para almacendar datos usando el "MERGE"

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE movie_demo.movie_merge;

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS movie_demo.movie_merge (
# MAGIC   movieId INT,
# MAGIC   Title  STRING,
# MAGIC   yearreleaseDate INT,
# MAGIC   ReleaseDate DATE,
# MAGIC   durationtime INT,
# MAGIC   createDate DATE,
# MAGIC   updateDate DATE
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC # MERGE PARA DIA 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from movie_demo.movie_day1 limit 2

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC     
# MAGIC MERGE INTO movie_demo.movie_merge tgt                     ---nombre de tabla
# MAGIC USING movie_day1 src                                      ---nombre de vista
# MAGIC ON tgt.movieId = src.movieId                              ---amarre
# MAGIC WHEN MATCHED THEN                                         --- cuando exista una coincidencia
# MAGIC   UPDATE SET                           
# MAGIC     tgt.Title = src.Title,
# MAGIC     tgt.yearreleaseDate = src.yearreleaseDate,
# MAGIC     tgt.ReleaseDate = src.ReleaseDate,
# MAGIC     tgt.durationtime = src.durationtime,
# MAGIC     tgt.updateDate = current_timestamp()
# MAGIC WHEN NOT MATCHED THEN                                     --- cuando no exista una coincidencia
# MAGIC    INSERT                                                 --- actualizo los valores
# MAGIC     (movieId, Title, yearreleaseDate, ReleaseDate, durationtime, createDate)
# MAGIC   VALUES
# MAGIC    (src.movieId, src.Title, src.yearreleaseDate, src.ReleaseDate, src.durationtime, current_timestamp())

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_demo.movie_merge

# COMMAND ----------

# MAGIC %md
# MAGIC # dia 2

# COMMAND ----------

# MAGIC %sql
# MAGIC     
# MAGIC MERGE INTO movie_demo.movie_merge tgt                     ---nombre de tabla
# MAGIC USING movie_day2 src                                      ---nombre de vista
# MAGIC ON tgt.movieId = src.movieId                              ---amarre
# MAGIC WHEN MATCHED THEN                                         --- cuando exista una coincidencia
# MAGIC   UPDATE SET                           
# MAGIC     tgt.Title = src.Title,
# MAGIC     tgt.yearreleaseDate = src.yearreleaseDate,
# MAGIC     tgt.ReleaseDate = src.ReleaseDate,
# MAGIC     tgt.durationtime = src.durationtime,
# MAGIC     tgt.updateDate = current_timestamp()
# MAGIC WHEN NOT MATCHED THEN                                     --- cuando no exista una coincidencia
# MAGIC    INSERT                                                 --- actualizo los valores
# MAGIC     (movieId, Title, yearreleaseDate, ReleaseDate, durationtime, createDate)
# MAGIC   VALUES
# MAGIC    (src.movieId, src.Title, src.yearreleaseDate, src.ReleaseDate, src.durationtime, current_timestamp())

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_demo.movie_merge

# COMMAND ----------

# MAGIC %md
# MAGIC # DIA 3 - LENGUAGE PYTHON

# COMMAND ----------

from pyspark.sql.functions import set

# COMMAND ----------

from delta.tables import *

deltaTablePeople = DeltaTable.forPath(spark, '/mnt/moviehistorico/demo/movie_merge')


deltaTablePeople.alias('tgt') \
  .merge(
    movie_day3_df.alias('src'),
    'tgt.movieId = src.movieId'
  ) \
  .whenMatchedUpdate(set =
    {
      "Title": "src.Title",
      "yearreleaseDate": "src.yearreleaseDate",
      "ReleaseDate": "src.ReleaseDate",
      "durationtime": "src.durationtime",
      "updateDate": "current_timestamp()"
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "movieId": "src.movieId",
      "Title": "src.Title",
      "yearreleaseDate": "src.yearreleaseDate",
      "ReleaseDate": "src.ReleaseDate",
      "durationtime": "src.durationtime",
      "createDate": "current_timestamp()"
    }
  ) \
  .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_demo.movie_merge

# COMMAND ----------

# MAGIC %md
# MAGIC # History, Time Travel y Vacuum.
# MAGIC
# MAGIC  * 1.- Historia y control de Versiones.
# MAGIC  * 2.- viaje en el tiempo.
# MAGIC  * 3.- Vacio. 

# COMMAND ----------

# MAGIC %md
# MAGIC * sentencia en sql saca por orden de verion todo los movimientos que se hicieron en la tabla, operation= el tipo de logica se utilizo para actualizar la tabla:

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC DESC HISTORY movie_demo.movie_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC * Esta sentencia saca en sql el historial de la tabla con todo los registros en esta version 1.-

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_demo.movie_merge VERSION AS OF 2;

# COMMAND ----------

# MAGIC %md
# MAGIC *  Consulta de sql sobre una fecha determinada, luego que traigo el "desc + history" por el campo timestamp.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_demo.movie_merge TIMESTAMP AS OF '2025-12-19T23:13:35.000+00:00';

# COMMAND ----------

# Creando un dataframe desde el delta table del merge.
df = spark.read.format("delta").option("timestampAsOf", '2025-12-19T23:13:35.000+00:00').load("/mnt/moviehistorico/demo/movie_merge")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC * sentencia lo que hace es vaciar la histori de esta tabla con "VACUUM". (Por defult el systema lo eliminar recien en una semana).

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC VACUUM movie_demo.movie_merge; 

# COMMAND ----------

# MAGIC %md
# MAGIC * Con esta sentencia lo que hace es vaciar la historia de esta tabla con "VACUUM" de forma inmediata. osea ya no poder acceder a los registros desde la segunda carga en adelante.

# COMMAND ----------

# DBTITLE 1,AIN
# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM movie_demo.movie_merge RETAIN 0 HOURS;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_demo.movie_merge TIMESTAMP AS OF '2025-12-19T23:13:35.000+00:00';

# COMMAND ----------

# consultamos nuevamente la historia, vemos que si aparece el detalle, mas no la descripcion cuando validamos por version.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY movie_demo.movie_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM movie_demo.movie_merge
# MAGIC WHERE yearreleaseDate = 2004;  

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY movie_demo.movie_merge;

# COMMAND ----------

 * Validamos que luego de eliminar 4 regsitros de la tabla, tenemos solo 33 registros. 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_demo.movie_merge

# COMMAND ----------

# MAGIC %md
# MAGIC  * lo que haremos sera, restaruar con "Merge into"los 4 registros de la tabla merge pero en una version antes de que se elimino.

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO movie_demo.movie_merge tg
# MAGIC USING movie_demo.movie_merge VERSION AS OF 9 src
# MAGIC ON tg.movieId = src.movieId
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *

# COMMAND ----------

# MAGIC %md
# MAGIC  * Validamos que la tabla de Merge nuevamente tiene insertado 37 registros.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_demo.movie_merge

# COMMAND ----------

# MAGIC %md
# MAGIC # Transaction Log en Delta Lake

# COMMAND ----------

# MAGIC    %sql
# MAGIC CREATE TABLE IF NOT EXISTS movie_demo.movie_log(
# MAGIC   movieId INT,
# MAGIC   Title  STRING,
# MAGIC   yearreleaseDate INT,
# MAGIC   ReleaseDate DATE,
# MAGIC   durationtime INT,
# MAGIC   createDate DATE,
# MAGIC   updateDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY movie_demo.movie_log

# COMMAND ----------

# MAGIC %md
# MAGIC * haciendo un insert a la tabla log.

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO movie_demo.movie_log
# MAGIC SELECT * FROM movie_demo.movie_merge
# MAGIC where movieId = '125537' 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_demo.movie_log

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY movie_demo.movie_log;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO movie_demo.movie_log
# MAGIC SELECT * FROM movie_demo.movie_merge
# MAGIC where movieId = '133575' 

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY movie_demo.movie_log;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM movie_demo.movie_log
# MAGIC WHERE movieId = '125537'

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY movie_demo.movie_log;

# COMMAND ----------

# MAGIC %md
# MAGIC * Desde un comando PYTHON creamos una lista y un for para insertarr registros a la tabla movie_log.

# COMMAND ----------

list = [118452, 124606, 125052, 125123, 125263, 125537, 126141, 133575, 142132, 146269, 157185]
for movieId in list:
    spark.sql(f"""INSERT INTO movie_demo.movie_log
	          SELECT * FROM movie_demo.movie_merge
			  WHERE movieId = {movieId}""")

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO movie_demo.movie_log
# MAGIC SELECT * FROM movie_demo.movie_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC #Convertir formato parquet a Delta

# COMMAND ----------

#Creando una tabla en formato parquet VACIO.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE movie_demo.movie_convert_to_data

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS movie_demo.movie_convert_to_delta(
# MAGIC   movieId INT,
# MAGIC   Title  STRING,
# MAGIC   yearreleaseDate INT,
# MAGIC   ReleaseDate DATE,
# MAGIC   durationtime INT,
# MAGIC   createDate DATE,
# MAGIC   updateDate DATE
# MAGIC )
# MAGIC USING PARQUET

# COMMAND ----------

# MAGIC %md
# MAGIC  insertando desde otra tabla a la tabla convert_to_data*

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO movie_demo.movie_convert_to_delta
# MAGIC SELECT * FROM movie_demo.movie_merge;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC * Haciendo la conversion de una carpeta parquet a una delta. (_delta_log)

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA movie_demo.movie_convert_to_delta;

# COMMAND ----------

# MAGIC %md
# MAGIC * se crea un df desde una sentencia spark. de una tabla sql

# COMMAND ----------

df = spark.table("movie_demo.movie_convert_to_delta")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC *creacion de dataframe en la ruta de la carreta delta_new en formato parquet

# COMMAND ----------

df.write.format("parquet").save("/mnt/moviehistorico/demo/movie_convert_to_delta_new")

# COMMAND ----------

# MAGIC %md
# MAGIC *creacion de dataframe en la ruta de la carreta delta_new en formato DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA parquet.`/mnt/moviehistorico/demo/movie_convert_to_delta_new` 

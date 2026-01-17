-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Acceso al Dataframe mediante SQL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC  # Objetivos:
-- MAGIC   * 1.- Documentacion sobre Spark SQL.
-- MAGIC   * 2.- Crear la Base de Datos "DEMO".
-- MAGIC   * 3.- Acceder al **"CATALOGO"** en la interfas del usuario.
-- MAGIC   * 4.- Comando **"SHOW"**.
-- MAGIC   * 5.- Comando **"Describe"** 
-- MAGIC   * 6.- Mostrar la Base de Datos Actual.

-- COMMAND ----------

---Creamos la base de datos demo / (IF NOT EXISTS)si ya existe ya no crearlo .
CREATE SCHEMA IF NOT EXISTS demo; 

-- COMMAND ----------

---Consultamos las base de datos existentes.
SHOW DATABASES;

-- COMMAND ----------

---Consultamos la base de datos demo.
DESCRIBE DATABASE demo;

-- COMMAND ----------

---Consultamos la base de datos extendida demo.
DESCRIBE DATABASE EXTENDED demo;

-- COMMAND ----------

--Consulto la BD actual donde estoy situado.
SELECT current_database();

-- COMMAND ----------

--Consultamos las tablas creadas en la BD por default
SHOW TABLES;

-- COMMAND ----------

--Consulto las tablas creadas en la BD "demo"
SHOW TABLES IN demo;

-- COMMAND ----------

--- Modifico en la BD que quiero trabajar.
USE demo;

-- COMMAND ----------

--Consulto la BD actual donde estoy situado.
SELECT current_database();

-- COMMAND ----------

---Consulto las tablas en la BD "default" aunque no este situdao ahi.
SHOW TABLES IN default;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Tablas Administradas(Managed Tables) 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC  * 1.- Crear una Tabla Administradas(Managed Tables) con "Python".
-- MAGIC  * 2.- Crear una Tabla Administradas(Managed Tables) con "SQL".
-- MAGIC  * 3.- Efecto de Eliminar una tabla Administrada
-- MAGIC  * 4.- Describir(Describe)la Tabla. 

-- COMMAND ----------

-- MAGIC %run "../03_includes/configuration"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC -llamamos al comando magico de python, ya estamos en sql / creamos una variable de parquet de la capa gold.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC results_movie_genre_languaje = spark.read.parquet(f"{gold_folder_path}/results_movie_genre_language")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC  * con la sentencia de python se crea una tabla en formato parquet, en la bd demo.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC results_movie_genre_languaje.write.format("parquet").saveAsTable("demo.results_movie_genre_languaje_python")

-- COMMAND ----------

---Consultado con sentencia SQL la bd "demo" y todos las tablas creadas.
USE demo;
SHOW TABLES;

-- COMMAND ----------

--- consulto su descripcion de la tabla y su extension de la misma.
describe extended results_movie_genre_languaje_python;

-- COMMAND ----------

--Creando un Tabla con sentencia sql y usando un filtro por el campo "genre_name" en la BD "demo".
CREATE TABLE demo.results_movie_genre_languaje_sql
AS
SELECT * FROM results_movie_genre_languaje_python
WHERE genre_name='Adventure'

-- COMMAND ----------

SELECT * FROM demo.results_movie_genre_languaje_sql

-- COMMAND ----------

select current_database()

-- COMMAND ----------

--- consulto su descripcion de la tabla y su extension de la misma.
describe extended demo.results_movie_genre_languaje_sql;

-- COMMAND ----------

drop table demo_results_movie_genre_languaje_sql;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Tablas Externas(External Tables)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC  * 1.- Crear una Tabla Externa(External Table) con "Python".
-- MAGIC  * 2.- Crear una Tabla Externa(External Table) con "SQL".
-- MAGIC  * 3.- Efecto de Eliminar una Tabla Externa(External Table).
-- MAGIC  * 4.- Describir(Describe)la Tabla. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC  * con el comando magico de python, traigo el formato parquet, de la la capa gold, option(ruta donde guardare el parquet) de la BD demo, el pongo el acronimo "py" de python.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC results_movie_genre_languaje.write.format("parquet").option("path", f"{gold_folder_path}/results_movie_genre_languaje_py").saveAsTable("demo.results_movie_genre_languaje_py")

-- COMMAND ----------

desc extended results_movie_genre_languaje_py

-- COMMAND ----------

SELECT * FROM results_movie_genre_languaje_py

-- COMMAND ----------

drop table results_movie_genre_languaje_sql;

-- COMMAND ----------

-- Creo una tabla con comando sql / lo defino como parquet "usin" / location / ruta de destino
Create table demo.results_movie_genre_languaje_sql (
  title STRING,
  duration_time INT,
  release_date DATE,
  vote_average FLOAT,
  language_name STRING,
  genre_name STRING,
  create_date timestamp
)
USING PARQUET
LOCATION "/mnt/moviehistorico/gold/results_movie_genre_languaje_ext_sql"


-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

--- Inserto de la BD "demo" la tabla "results_movie_genre_languaje_py" en la tabla  ------"results_movie_genre_languaje_sql".

INSERT INTO demo.results_movie_genre_languaje_sql
SELECT * FROM demo.results_movie_genre_languaje_py
WHERE genre_name = "Adventure"

-- COMMAND ----------

SELECT count(*) FROM demo.results_movie_genre_languaje_sql

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Vistas(View)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC -- Objetivos:
-- MAGIC  * 1.- Crear Vista Temporal.
-- MAGIC  * 2.- Crear Vista Temporal Global.
-- MAGIC  * 3.- Crear Vista Permanente. 

-- COMMAND ----------

select current_database(); 

-- COMMAND ----------

--Creo la vista temporal - si ya existe la reeplaza al volver a ejecutar.
CREATE OR REPLACE TEMP VIEW v_results_movie_genre_language
AS 
SELECT *
FROM demo.results_movie_genre_languaje_py
WHERE genre_name = "Adventure";

-- COMMAND ----------

--- Una vista no ocupa espacio en el disco / solo se puede acceder en el mismo notebook.
SELECT * FROM v_results_movie_genre_language

-- COMMAND ----------

--Creo la vista global - si ya existe la reemplaza al volver a ejecutar.
CREATE OR REPLACE GLOBAL TEMP VIEW gv_results_movie_genre_language
AS 
SELECT *
FROM demo.results_movie_genre_languaje_py
WHERE genre_name = "Drama";

-- COMMAND ----------

---para encontar el esquema que se debe encontrar el global_temp
SHOW TABLES IN global_temp;

-- COMMAND ----------

---Para la conuslta se debe ateponer primero su esquema.
SELECT * FROM global_temp.gv_results_movie_genre_language

-- COMMAND ----------

--Creo la vista permanente - si ya existe la reemplaza al volver a ejecutar.
CREATE OR REPLACE VIEW pv_results_movie_genre_language
AS 
SELECT *
FROM demo.results_movie_genre_languaje_py
WHERE genre_name = "Comedy";

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- Databricks notebook source
SHOW DATABASES;

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

----Modificando en la BD que voy a trabajar
USE movie_silver;

-- COMMAND ----------

----consultando todas las tablas en la BD MOVIE_SILVER.
SHOW TABLES

-- COMMAND ----------

---consultando una tabla por su nombre de esquema y nombre de tabla:
SELECT * FROM movie_silver.movie

-- COMMAND ----------

---sancado dscripcion de la tabla movie
DESCRIBE MOVIE;

-- COMMAND ----------

----sacando la cantida que quiero que traiga la tabla.
select * from movie
limit 100;

-- COMMAND ----------

SELECT * FROM MOVIE
WHERE year_release_date = 2015 and renueve is not null
order by duration_time DESC;

-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Scalar Functions (Funciones Escalares) 

-- COMMAND ----------

use movie_silver

-- COMMAND ----------

SELECT 
*, CONCAT(title, '-', release_date) as title_with_release_date
FROM movie
limit 10;

-- COMMAND ----------

--- La funcion "SPLIT" es como la funcion "SUBTRING" te trae de la posiciion que necesesita, forename = primera posiciion, surname= segunda posicion
SELECT 
*, SPLIT(name, ' ')[0] forename, SPLIT(name, ' ')[2] surname
FROM persons
limit 10;

-- COMMAND ----------

--- trayendo un campo fecha del system.
SELECT 
*, current_timestamp() as Fecha_Update 
FROM persons
limit 10;

-- COMMAND ----------

---formateando un campo fecha a: DD
SELECT 
*, date_format(release_date, 'dd-MM-yyyy') as Fecha_formato
FROM movie
limit 10;

-- COMMAND ----------

---Agregando campo con la condicion del campo fecha + un dia al nuevo campo.

SELECT 
*, date_add(release_date, 1) as Fecha_Agregada
FROM movie
limit 10;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Aggregate Functions(Funciones de Agregacion)

-- COMMAND ----------

SELECT COUNT(*) FROM movie

-- COMMAND ----------

SELECT 
MAX(release_date)
FROM movie

-- COMMAND ----------


--Fecha calculado
SELECT year_release_date,COUNT(1) as Total_count, SUM(budget) AS sum_budget,
  Max(budget) AS Max_budget, AVG(budget) AS avg_budget
FROM movie
GROUP BY year_release_date
HAVING COUNT(1) > 220 
ORDER BY year_release_date DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Window Functions (Funciones de Ventana)

-- COMMAND ----------

-- Sacanndo un ranking y particionado por año de pelicula, ordenado por "release_date".
SELECT Title, year_release_date, release_date,
   RANK() OVER(PARTITION BY year_release_date ORDER BY release_date) AS rank 
FROM movie
WHERE year_release_date IS NOT NULL
ORDER BY release_date

-- COMMAND ----------

SELECT * FROM movie LIMIT 10

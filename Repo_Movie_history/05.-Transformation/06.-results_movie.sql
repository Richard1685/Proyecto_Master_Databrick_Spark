-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Crear la tabla "results_movie" en la capa gold.   

-- COMMAND ----------

use movie_silver;

-- COMMAND ----------

DROP TABLE IF EXISTS movie_gold.results_movie;

CREATE TABLE movie_gold.results_movie
USING parquet
AS
SELECT
M.year_release_date, C.country_name, PCO.company_name, M.budget, M.renueve 
FROM movie M
INNER JOIN production_country  PC  ON  M.movie_id    = PC.movie_id   
INNER JOIN countries              C  ON  C.country_id  = PC.country_id
INNER JOIN movie_company         MC  ON  M.movie_id    = MC.movie_id
INNER JOIN production_company   PCO  ON  MC.company_id = PCO.company_id

-- COMMAND ----------

SELECT * FROM movie_gold.results_movie

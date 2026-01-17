-- Databricks notebook source
USE movie_gold;

-- COMMAND ----------

---SACANDO PROMEDIO Y CASTEANDO LOS DECIMALES DEL CAMPO PROMEDIADO.
SELECT 
country_name,
COUNT(country_name) AS Total_Movie,
SUM(budget) AS Total_budget,
CAST(AVG(budget) AS decimal(18,2)) AS AVG_Budget,
SUM(renueve) AS Total_renueve,
CAST(AVG(renueve) AS decimal(18,2))  AS AVG_renueve,
date_format(current_timestamp(), 'yyyy-MM-dd') AS fecha_system
FROM movie_gold.results_movie
GROUP BY country_name
ORDER BY Total_renueve DESC
LIMIT 10;

-- COMMAND ----------

---SACANDO PROMEDIO Y CASTEANDO LOS DECIMALES DEL CAMPO PROMEDIADO.
----traigo un campo fecha del systema, formateando por año-mes-dia
SELECT country_name,
COUNT(country_name) AS Total_Movie,
SUM(budget) AS Total_budget,
CAST(AVG(budget) AS decimal(18,2)) AS AVG_Budget,
SUM(renueve) AS Total_renueve,
CAST(AVG(renueve) AS decimal(18,2))  AS AVG_renueve,
date_format(current_timestamp(), 'yyyy-MM-dd') AS fecha_system
FROM movie_gold.results_movie
WHERE year_release_date BETWEEN 2010 and 2015
GROUP BY country_name
ORDER BY Total_renueve DESC
LIMIT 10;

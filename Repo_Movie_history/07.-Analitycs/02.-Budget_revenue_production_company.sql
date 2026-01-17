-- Databricks notebook source
select current_database()

-- COMMAND ----------

USE movie_gold;

-- COMMAND ----------

SELECT company_name,
COUNT(company_name) AS Total_company_name,
SUM(budget) AS Total_budget,
CAST(AVG(budget) AS decimal(18,2)) AS AVG_Budget,
SUM(renueve) AS Total_renueve,
CAST(AVG(renueve) AS decimal(18,2))  AS AVG_renueve,
date_format(current_timestamp(), 'yyyy-MM-dd') AS fecha_system
FROM movie_gold.results_movie
GROUP BY company_name
ORDER BY Total_renueve DESC

-- COMMAND ----------

SELECT company_name,
COUNT(company_name) AS Total_company_name,
SUM(budget) AS Total_budget,
CAST(AVG(budget) AS decimal(18,2)) AS AVG_Budget,
SUM(renueve) AS Total_renueve,
CAST(AVG(renueve) AS decimal(18,2))  AS AVG_renueve,
year_release_date,
date_format(current_timestamp(), 'yyyy-MM-dd') AS fecha_system
FROM movie_gold.results_movie
WHERE year_release_date BETWEEN 2010 AND 2015
GROUP BY company_name,year_release_date
ORDER BY Total_renueve DESC

-- Databricks notebook source
-- MAGIC %md
-- MAGIC # JOIN

-- COMMAND ----------

USE movie_silver;

-- COMMAND ----------

DESCRIBE movie;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_movie_2011
AS
SELECT movie_id, Title, budget, renueve, year_release_date, release_date
FROM movie
WHERE year_release_date = 2011;


-- COMMAND ----------

SELECT * FROM v_movie_2011;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_production_country
AS
SELECT * FROM production_country;

-- COMMAND ----------

SELECT * FROM v_production_country
LIMIT 10;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_country
AS
SELECT 
country_id, country_name
FROM countries;

-- COMMAND ----------

SELECT * FROM v_country
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # INNER JOIN

-- COMMAND ----------

SELECT 
vm.Title, vm.budget, vm.renueve, vm.year_release_date, vm.release_date, vc.country_name
FROM v_movie_2011 vm
INNER JOIN v_production_country vpc ON vm.movie_id = vpc.movie_id
INNER JOIN v_country             vc ON vpc.country_id = vc.country_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # LEFT JOIN

-- COMMAND ----------

SELECT 
vm.Title, vm.budget, vm.renueve, vm.year_release_date, vm.release_date, vc.country_name
FROM v_movie_2011 vm
LEFT JOIN v_production_country vpc ON vm.movie_id = vpc.movie_id
LEFT JOIN v_country             vc ON vpc.country_id = vc.country_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # RIGTH JOIN

-- COMMAND ----------

SELECT 
vm.Title, vm.budget, vm.renueve, vm.year_release_date, vm.release_date, vc.country_name
FROM v_movie_2011 vm
RIGHT JOIN v_production_country vpc ON vm.movie_id = vpc.movie_id
RIGHT JOIN v_country             vc ON vpc.country_id = vc.country_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # FULL JOIN 

-- COMMAND ----------

SELECT 
vm.Title, vm.budget, vm.renueve, vm.year_release_date, vm.release_date, vc.country_name
FROM v_movie_2011 vm
FULL JOIN v_production_country vpc ON vm.movie_id = vpc.movie_id
FULL JOIN v_country             vc ON vpc.country_id = vc.country_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # SEMI JOIN

-- COMMAND ----------

SELECT 
vm.Title, vm.budget, vm.renueve, vm.year_release_date, vm.release_date
FROM v_movie_2011 vm
INNER JOIN v_production_country vpc ON vm.movie_id = vpc.movie_id
SEMI JOIN v_country             vc ON vpc.country_id = vc.country_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # ANTI JOIN - ESTA CONSULTA ES LO INVERSO AL SEMI JOIN

-- COMMAND ----------

SELECT 
vm.Title, vm.budget, vm.renueve, vm.year_release_date, vm.release_date
FROM v_movie_2011 vm
INNER JOIN v_production_country vpc ON vm.movie_id = vpc.movie_id
ANTI JOIN v_country             vc ON vpc.country_id = vc.country_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # CROSS JOIN (producto cartesiano de las vistas)

-- COMMAND ----------

SELECT 
vm.Title, vm.budget, vm.renueve, vm.year_release_date, vm.release_date, vc.country_name
FROM v_movie_2011 vm CROSS JOIN v_country vc;

-- Databricks notebook source
-- MAGIC %md
-- MAGIC

-- COMMAND ----------

DROP DATABASE movie_silver

-- COMMAND ----------

----Creando BD en el catalogo Hive-Mestatore /administrado por SPARK (databrick).
CREATE SCHEMA IF NOT EXISTS movie_silver
LOCATION "/mnt/moviehistorico/silver";

-- COMMAND ----------

DESC DATABASE movie_silver;

-- COMMAND ----------



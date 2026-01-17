-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Eliminar y volver a crear todas las Bases de datos.

-- COMMAND ----------

---Elimimando la BD Y sus tablas parquet en forma de cascada
DROP DATABASE IF EXISTS movie_silver CASCADE;

-- COMMAND ----------

-- Creando la BD y/o Squema - si existe la recree. / location = ubicacion en donde se creara la BD
CREATE SCHEMA IF NOT EXISTS movie_silver
LOCATION "/mnt/moviehistorico/silver";

-- COMMAND ----------

---Elimimando la Bd y sus tablas parquet en forma de cascada
DROP DATABASE IF EXISTS movie_gold CASCADE;

-- COMMAND ----------

-- Creando la BD y/o Squema - si existe la recree. / location = ubicacion en donde se creara la BD
CREATE SCHEMA IF NOT EXISTS movie_silver
LOCATION "/mnt/moviehistorico/gold";

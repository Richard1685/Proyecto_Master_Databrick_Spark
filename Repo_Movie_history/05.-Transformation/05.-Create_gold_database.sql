-- Databricks notebook source
---Creando BD Movie_gold en el Catalogo - HIVE-METASTORE / Consulta externa desde SPARk - Databrick.
CREATE SCHEMA if NOT EXISTS movie_gold
LOCATION "/mnt/moviehistorico/gold";

-- COMMAND ----------

DESC DATABASE movie_gold;

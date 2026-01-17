-- Databricks notebook source
----Creando BD para almacenar datos en el Catalogo - Hive-Mestatore (Spark externo)
CREATE SCHEMA IF NOT EXISTS movie_bronze;

-- COMMAND ----------

--Eliminando BD creado por error.
DROP DATABASE movie_broze;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Crear Tablas para archivos CSV de forma externa

-- COMMAND ----------

-- MAGIC %md
-- MAGIC  * Crear tabla "Movie".

-- COMMAND ----------

----Creando Tabla con sentencia SQL, trayendo el archivo 'CSV' de la capa bronze y llevarlo al catalogo - -------Hive-Mestatore / BD movie_bronze

DROP TABLE movie_bronze.movie;

CREATE TABLE IF NOT EXISTS movie_bronze.movie (
movieId INT,
title STRING,
budget DOUBLE,
homePage STRING,
overview STRING,
popularity DOUBLE,
yearRealeseDate INT,
revenue DOUBLE,
durationTime INT,
movieStatus STRING,
tagline STRING,
voteAverage DOUBLE,
voteCount INT  
)
USING CSV
OPTIONS (path "/mnt/moviehistorico/bronze/movie.csv", header true)

-- COMMAND ----------

----Creando Tabla con sentencia SQL, trayendo el archivo 'CSV' de la capa bronze y llevarlo al catalogo - -------Hive-Mestatore / BD movie_bronze
DROP TABLE IF EXISTS movie_bronze.languages;

CREATE TABLE IF NOT EXISTS movie_bronze.languages (
languageId INT,
languageCode STRING,
languageName STRING
)
USING CSV
OPTIONS (path "/mnt/moviehistorico/bronze/language.csv", header true)

-- COMMAND ----------

----Creando Tabla con sentencia SQL, trayendo el archivo 'CSV' de la capa bronze y llevarlo al catalogo - -------Hive-Mestatore / BD movie_bronze
DROP TABLE IF EXISTS movie_bronze.genre;

CREATE TABLE IF NOT EXISTS movie_bronze.genre (
genreId INT,
genreName STRING
)
USING CSV
OPTIONS (path "/mnt/moviehistorico/bronze/genre.csv", header true)

-- COMMAND ----------

SELECT * FROM movie_bronze.movie

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Crear Tablas para archivos JSON de forma externa

-- COMMAND ----------

-- MAGIC %md
-- MAGIC  * Crear la tabla "country".
-- MAGIC    - JSON de una sola Linea.
-- MAGIC    - Estructura simple.

-- COMMAND ----------

----Creando Tabla con sentencia SQL, trayendo el archivo 'JSON' de la capa bronze y llevarlo al catalogo - -------Hive-Mestatore / BD movie_bronze
DROP TABLE IF EXISTS movie_bronze.country;

CREATE TABLE IF NOT EXISTS movie_bronze.country (
countryId INT,
countryIsoCode STRING,
countryName STRING
)
USING JSON
OPTIONS (path "/mnt/moviehistorico/bronze/country.json")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC  * Crear la tabla "persons".
-- MAGIC   - JSON de una sola Linea.
-- MAGIC   - Estructura compleja.

-- COMMAND ----------

----Creando Tabla con sentencia SQL, trayendo el archivo 'JSON' de la capa bronze. 
---"STRUCT" = aqui definimos que el archivo json tiene forename y surname y le damos una estructura y formato.
----se lleva al catalogo Hive-Mestatore / BD movie_bronze

DROP TABLE IF EXISTS movie_bronze.person;

CREATE TABLE IF NOT EXISTS movie_bronze.person (
personId INT,
personName STRUCT<forename: STRING, surname: STRING>
)
USING JSON
OPTIONS (path "/mnt/moviehistorico/bronze/person.json")

-- COMMAND ----------

SELECT * FROM movie_bronze.person

-- COMMAND ----------

-- MAGIC %md
-- MAGIC  * Crear la tabla "movie_genre".
-- MAGIC   - JSON de una sola Linea.
-- MAGIC   - Estructura simple.

-- COMMAND ----------

----Creando Tabla con sentencia SQL, trayendo el archivo 'JSON' de la capa bronze y llevarlo al catalogo - -------Hive-Mestatore / BD movie_bronze

DROP TABLE IF EXISTS movie_bronze.movie_genre;

CREATE TABLE IF NOT EXISTS movie_bronze.movie_genre (
movieId INT,
genreId INT
)
USING JSON
OPTIONS (path "/mnt/moviehistorico/bronze/movie_genre.json")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * Crear la tabla "movie_cats".
-- MAGIC   - JSON MultiLinea.
-- MAGIC   - Estructura simple.

-- COMMAND ----------

----Creando Tabla con sentencia SQL, trayendo el archivo 'JSON' de la capa bronze y llevarlo al catalogo - 
---- en la "option" luego de la ruta origen, usamos "multiLine" para indicar que es un archivo multilinea.
-------Hive-Mestatore / BD movie_bronze

DROP TABLE IF EXISTS movie_bronze.movie_cast;

CREATE TABLE IF NOT EXISTS movie_bronze.movie_cast (
movieId INT,
personId INT,
characterName STRING,
genreId INT,
castOrder INT
)
USING JSON
OPTIONS (path "/mnt/moviehistorico/bronze/movie_cast.json", multiLine true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC  * Crear la tabla "language_role".
-- MAGIC   - JSON MultiLinea.
-- MAGIC   - Estructura simple.

-- COMMAND ----------

----Creando Tabla con sentencia SQL, trayendo el archivo 'JSON' de la capa bronze y llevarlo al catalogo - 
---- en la "option" luego de la ruta origen, usamos "multiLine" para indicar que es un archivo multilinea.
-------Hive-Mestatore / BD movie_bronze

DROP TABLE IF EXISTS movie_bronze.language_role;

CREATE TABLE IF NOT EXISTS movie_bronze.language_role (
roleId INT,
languageRole STRING
)
USING JSON
OPTIONS (path "/mnt/moviehistorico/bronze/language_role.json", multiLine true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Crear Tablas para lista de archivos (CSVs - JSONs) de forma externa.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * Crear la tabla "production_company".
-- MAGIC   - Archivo CSV.
-- MAGIC   - Multiple Archivos.

-- COMMAND ----------

----Creando Tabla con sentencia SQL, trayendo el archivo 'CSV' de la capa bronze y llevarlo al catalogo - 
---- en la "option" luego de la ruta origen, no usamos la extension porque viene desde una carpeta de la ruta.
-------Hive-Mestatore / BD movie_bronze

DROP TABLE IF EXISTS movie_bronze.production_company;

CREATE TABLE IF NOT EXISTS movie_bronze.production_company (
companyId INT,
companyName STRING
)
USING CSV
OPTIONS (path "/mnt/moviehistorico/bronze/production_company")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * Crear la tabla "movie_company".
-- MAGIC   - Archivo CSV.
-- MAGIC   - Multiple Archivos.

-- COMMAND ----------

----Creando Tabla con sentencia SQL, trayendo el archivo 'CSV' de la capa bronze y llevarlo al catalogo - 
---- en la "option" luego de la ruta origen, no usamos la extension porque viene desde una carpeta de la ruta.
-----Hive-Mestatore / BD movie_bronze

DROP TABLE IF EXISTS movie_bronze.movie_company;

CREATE TABLE IF NOT EXISTS movie_bronze.movie_company (
movieId INT,
companId INT
)
USING CSV
OPTIONS (path "/mnt/moviehistorico/bronze/movie_company")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC %md
-- MAGIC * Crear la tabla "movie_language".
-- MAGIC   - Archivo JSON Multilinea.
-- MAGIC   - Multiple Archivos.

-- COMMAND ----------

----Creando Tabla con sentencia SQL, trayendo el archivo 'JSON' de la capa bronze y llevarlo al catalogo - 
---- en la "option" luego de la ruta origen, no usamos la extension porque viene desde una carpeta de la ruta.
-----usamos "multiLine" para indicar que es un archivo multilinea - Hive-Mestatore / BD movie_bronze

DROP TABLE IF EXISTS movie_bronze.movie_language;

CREATE TABLE IF NOT EXISTS movie_bronze.movie_language (
movieId INT,
languageId INT,
languageRoleId INT
)
USING JSON
OPTIONS (path "/mnt/moviehistorico/bronze/movie_language", multiLine true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC  * Crear la tabla "production_country".
-- MAGIC   - Archivo JSON Multilinea.
-- MAGIC   - Multiple Archivos.

-- COMMAND ----------

----Creando Tabla con sentencia SQL, trayendo el archivo 'JSON' de la capa bronze y llevarlo al catalogo - 
---- en la "option" luego de la ruta origen, no usamos la extension porque viene desde una carpeta de la ruta.
-----usamos "multiLine" para indicar que es un archivo multilinea - Hive-Mestatore / BD movie_bronze

DROP TABLE IF EXISTS movie_bronze.production_country;

CREATE TABLE IF NOT EXISTS movie_bronze.production_country (
movieId INT,
countryId INT
)
USING JSON
OPTIONS (path "/mnt/moviehistorico/bronze/production_country", multiLine true)

-- COMMAND ----------

SELECT * FROM movie_bronze.production_country

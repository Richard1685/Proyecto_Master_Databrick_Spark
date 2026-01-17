# Databricks notebook source
v_result = dbutils.notebook.run("01.-Ingestion_file_movie", 30, {"p_environment": "developer", "p_file_date":
    "2024-12-16"})
v_result


# COMMAND ----------

v_result = dbutils.notebook.run("02.-Ingestion_file_language", 30, {"p_environment": "developer", "p_file_date":
"2024-12-16"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("03.-Ingestion_file_genre", 30, {"p_environment": "developer", "p_file_date":
"2024-12-16"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("04.-Ingestion_file_country", 30, {"p_environment": "developer", "p_file_date":
    "2024-12-16"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("05.-Ingestion_file_person", 30, {"p_environment": "developer", "p_file_date":
    "2024-12-16"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("06.-Ingestion_file_movie_genre", 30, {"p_environment": "developer", "p_file_date":
    "2024-12-16"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("07.-Ingestion_file_movie_cast", 30, {"p_environment": "developer", "p_file_date":
    "2024-12-16"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("08.-Ingestion_file_language_role", 30, {"p_environment": "developer", "p_file_date": "2024-12-16"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run(
    "09.-Ingestion_folder_production_company", 
    30,  # Timeout de 10 minutos
    {"p_environment": "developer", "p_file_date": "2024-12-16"}
)
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("10.-Ingestion_folder_movie_company", 30, {"p_environment": "developer", "p_file_date": "2024-12-16"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("11.-Ingestion_folder_movie_language", 30, {"p_environment": "developer", "p_file_date": "2024-12-16"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("12.-Ingestion_folder_production_country", 30, {"p_environment": "developer", "p_file_date": "2024-12-16"})
v_result

# Databricks notebook source
# MAGIC %sql
# MAGIC ---se valida que usando sql desde otro notebook no se puede llamar a una vista creada.
# MAGIC SELECT * FROM v_movie_genre_language

# COMMAND ----------

# MAGIC %sql
# MAGIC ---se valida que usando sql desde otro notebook nosi se puede llamar a una vista con su esquema global creada.
# MAGIC SELECT * FROM global_temp.gv_movie_genre_language

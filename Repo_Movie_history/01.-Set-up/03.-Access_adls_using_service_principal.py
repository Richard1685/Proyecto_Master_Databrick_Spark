# Databricks notebook source
# MAGIC %md
# MAGIC # Acceder a Azure Data Lake Storage mediante Access Key
# MAGIC
# MAGIC 1.- Registrar la aplicacion  en azure entra ID \Service Principal.
# MAGIC 2.- Generar un secreto (contraseña)para la aplicación.
# MAGIC 3.- Configurar Spark co APP Client id,Directory /Tenand & secret.
# MAGIC 4.- Asignar el Role "Storage Blob Data Contributor" al Data Lake. 

# COMMAND ----------

# Considerar que estos scope se crearon de unos codigos uncio y se le dieron una variable a cada id.
  # en azure key vault opcion secrets + en databrick el enlance creas el scope.

client_id = dbutils.secrets.get(scope = "movie-history-secret-scope", key = "client-id")
tenant_id = dbutils.secrets.get(scope = "movie-history-secret-scope", key = "tenant-id")
client_secret = dbutils.secrets.get(scope = "movie-history-secret-scope", key = "client-secret")


# COMMAND ----------


spark.conf.set("fs.azure.account.auth.type.moviehistorico.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.moviehistorico.dfs.core.windows.net",
                "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.moviehistorico.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.moviehistorico.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.moviehistorico.dfs.core.windows.net", 
               f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@moviehistorico.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@moviehistorico.dfs.core.windows.net/movie.csv"))

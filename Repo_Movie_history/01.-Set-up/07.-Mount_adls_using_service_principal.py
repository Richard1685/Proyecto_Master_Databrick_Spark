# Databricks notebook source
# MAGIC %md
# MAGIC # Mount(montar) Azure Data Lake mendiante service Principal
# MAGIC
# MAGIC 1.- Obtener el valor client_id, tenant_id y cliente_secret del key vault.
# MAGIC 2.- Configurar Spark con APP/Client id,Directory/Tenant id & Secret.
# MAGIC 3.- Utilizar el metodo "monunt"de Utility para montar el almacenamiento.
# MAGIC 4.- Explorar otras utilidades del sistema de archivos relacionados con el montaje(list all,mounts, unmounts).
# MAGIC

# COMMAND ----------

client_id = dbutils.secrets.get(scope = "movie-history-secret-scope", key = "client-id")
tenant_id = dbutils.secrets.get(scope = "movie-history-secret-scope", key = "tenant-id")
client_secret = dbutils.secrets.get(scope = "movie-history-secret-scope", key = "client-secret")


# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
         "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

# 1. Bloque de Desmontaje (Unmount) ---este paso es si ya existe el montaje
mount_point = "/mnt/moviehistorico/demo" # Definir la variable aquí para que sea accesible
if any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
      dbutils.fs.unmount(mount_point)


# 2. Bloque de Montaje (Mount) ---este paso es para crear un montaje
# Nótese la barra diagonal (/) al final de la fuente
dbutils.fs.mount(
  source = "abfss://demo@moviehistorico.dfs.core.windows.net",
  mount_point = "/mnt/moviehistorico/demo",
  extra_configs = configs)

# COMMAND ----------

# consultando en mi (Adls gen2 = moviehistorico) - mi carpeta demo.
display(dbutils.fs.ls("/mnt/moviehistorico/demo"))

# COMMAND ----------

# consultando en mi (storage account = moviehistorico) - mi cintenedor "demo" y archivo movie.csv.
display(spark.read.csv("/mnt/moviehistorico/demo/movie.csv"))

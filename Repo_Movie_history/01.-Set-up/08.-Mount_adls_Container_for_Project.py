# Databricks notebook source
# MAGIC %md
# MAGIC # Mount(montar) Azure Data Lake para Proyecto
# MAGIC

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    # Obtener los secretos key de key vault
    client_id = dbutils.secrets.get(scope = "movie-history-secret-scope", key = "client-id")
    tenant_id = dbutils.secrets.get(scope = "movie-history-secret-scope", key = "tenant-id")
    client_secret = dbutils.secrets.get(scope = "movie-history-secret-scope", key = "client-secret")

    # Establecer configuraciones de spark
    configs = {"fs.azure.account.auth.type": "OAuth",
               "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
               "fs.azure.account.oauth2.client.id": client_id,
               "fs.azure.account.oauth2.client.secret": client_secret,
               "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"} # <-- Error tipográfico corregido
    
    mount_point = f"/mnt/{storage_account_name}/{container_name}"
    
    # Desmontar (unmount) si ya existe.
    if any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()): # <-- Corregido dbutils.fs.mounts()
          dbutils.fs.unmount(mount_point)

    # Mount(MONTAR) el contenedor del Storage Account
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/", # <-- Corregida la variable storage_account_name
        mount_point = mount_point,
        extra_configs = configs
    )

    # Listar los montajes
    display(dbutils.fs.mounts()) # <-- Corregido dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %md
# MAGIC # Montar(mount)hacia el contenedor "bronze"
# MAGIC

# COMMAND ----------

mount_adls("moviehistorico", "bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC # Montar(mount)hacia el contenedor "silver"

# COMMAND ----------

mount_adls("moviehistorico", "silver")

# COMMAND ----------

# MAGIC %md
# MAGIC #Montar(mount)hacia el contenedor "Gold"

# COMMAND ----------

mount_adls("moviehistorico", "gold")

# COMMAND ----------

mount_adls("moviehistorico", "gold")

# COMMAND ----------

# MAGIC %md
# MAGIC #Montar(mount)hacia el contenedor "demo"

# COMMAND ----------

mount_adls("moviehistorico", "demo")

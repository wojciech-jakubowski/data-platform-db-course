# Databricks notebook source
storage_account_name = dbutils.secrets.get(scope="KVSecretScope", key="DLStorageAccountName")
client_id = dbutils.secrets.get(scope="KVSecretScope", key="DataSpnClientId")
client_secret = dbutils.secrets.get(scope="KVSecretScope", key="DataSpnClientSecret")
tenant_id = dbutils.secrets.get(scope="KVSecretScope", key="TenantId")

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

container_name = "raw"
dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{container_name}",
  extra_configs = configs)

container_name = "processed"
dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{container_name}",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls dbfs:/mnt/

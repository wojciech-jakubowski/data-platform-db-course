# Databricks notebook source
#storage_account_name = dbutils.secrets.get(scope="KVSecretScope", key="DLStorageAccountName")
#client_id = dbutils.secrets.get(scope="KVSecretScope", key="DataSpnClientId")
#client_secret = dbutils.secrets.get(scope="KVSecretScope", key="DataSpnClientSecret")
#tenant_id = dbutils.secrets.get(scope="KVSecretScope", key="TenantId")

storage_account_name = "wjdl"
container_name = "raw"
client_id = "68d84eec-1cb6-4495-90d1-b725557f3a35"
client_secret = "Elt8Q~CmGwOv~LzD.Uw0nVMt9lc7geNU3~Nv.bDf"
tenant_id = "2ee548e1-6be8-4729-b86e-f482e29d2c9f"


configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{container_name}",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls dbfs:/mnt/

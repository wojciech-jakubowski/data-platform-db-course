# Databricks notebook source
dbutils.secrets.get(scope="KVSecretScope", key="TenantId")

# COMMAND ----------

storage_account_name = dbutils.secrets.get(scope="KVSecretScope", key="DLStorageAccountName")
client_id = dbutils.secrets.get(scope="KVSecretScope", key="DataSpnClientId")
client_secret = dbutils.secrets.get(scope="KVSecretScope", key="DataSpnClientSecret")
tenant_id = dbutils.secrets.get(scope="KVSecretScope", key="TenantId")

storage_account_name = "wjonedevdlsa"
client_id = "68d84eec-1cb6-4495-90d1-b725557f3a35"
client_secret = ""
tenant_id = "2ee548e1-6be8-4729-b86e-f482e29d2c9f"


configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": client_id,
           "fs.azure.account.oauth2.client.secret": client_secret,
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/" + tenant_id + "/oauth2/token"}

# COMMAND ----------

def mount_fs(container_name: str) -> None: 
    print("mounting...")
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}dfs.core.windows.net/",
        mount_point = f"/mnt/{container_name}",
        extra_configs = configs)
    print("mounted!")

# COMMAND ----------

mount_fs("raw")

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls dbfs:/mnt/

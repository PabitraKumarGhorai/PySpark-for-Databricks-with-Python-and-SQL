# Databricks notebook source
container_name = "curated"
storage_account = "rxglobaladlsdevtestgen2"
application_id = "2ee38471hgyio-7484-7304-sd1"
tenantID/directory_id = "6334-gfhtr-9873-dfhdjsks"
secret = ""

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
       "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
       "fs.azure.account.oauth2.client.id": "<appId>",
       "fs.azure.account.oauth2.client.secret": "<clientSecret>",
       "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<tenantId>/oauth2/token",
       "fs.azure.createRemoteFileSystemDuringInitialization": "true"}

# Optionally, you can add <directory-name> to the source URI of your mount point.

dbutils.fs.mount(
source = "abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/<directory-name>",
mount_point = "/mnt/<mount-point_name>",
extra_configs = configs)

# COMMAND ----------

# Un-mount a mount point
# dbutils.fs.unmount('/mnt/<mount-point>')
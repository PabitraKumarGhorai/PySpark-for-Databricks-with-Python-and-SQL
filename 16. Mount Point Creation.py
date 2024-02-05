# Databricks notebook source
# Mount Point --> Mount point is a kind of masking of storage account details.

# We need --> 1. Storage Account 2. Cointainer Name 3. Access Key



# COMMAND ----------

container_name = "Rx_Container"

storage_account_name = "rxglobalblob"

mount_name = "Rx-Container"

conf_key = "fs.azure.account.key.<storage_account_name>.blob.core.windows.net"
key_name = "Ldsdskcldser348ycjdscjdshQeeAAD==="

dbutils.fs.mount(
    source = "wasb://<container_name>@<storage_account_name>.blob.core.windows.net",
    mount_point = "/mnt/<mount_name>",
    extra_configs = {"<conf_key>:<key_name>"}
)


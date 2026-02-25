# Databricks notebook source
storage_account = "anniedatalake123"
container = "annie-data"
access_key = "<YOUR_STORAGE_ACCOUNT_KEY>"

spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    access_key
)

# COMMAND ----------

dbutils.fs.ls(
    f"abfss://{container}@{storage_account}.dfs.core.windows.net/"
)

# COMMAND ----------

raw_path = "abfss://annie-data@anniedatalake123.dfs.core.windows.net/raw/"
dbutils.fs.ls(raw_path)
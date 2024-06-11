# Databricks notebook source
dbutils.fs.mount(
  source = "wasbs://raw@adlscloudthat.blob.core.windows.net",
  mount_point = "/mnt/adlscloudthat/raw",
  extra_configs = {"fs.azure.account.key.adlscloudthat.blob.core.windows.net":"secretkey"})

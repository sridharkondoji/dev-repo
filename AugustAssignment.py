# Databricks notebook source
import pyspark
from pyspark.sql.functions import to_date, col

#Pre Uploaded csv files to FileStore
devices_file_location = "/FileStore/tables/device_data.csv"
history_file_location = "/FileStore/tables/history_data.csv"

#Renaming columns of devices and history data
devices_df = spark.read.format("csv").option("inferSchema", "true").option("header", "true").option("sep", ",").load(devices_file_location)
history_df = spark.read.format("csv").option("inferSchema", "true").option("header", "true").option("sep", ",").load(history_file_location)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS sensor_db COMMENT 'This is customer database' LOCATION '/user'

# COMMAND ----------

spark.sql("drop table if exists sensor_db.devices")
spark.sql("drop table if exists sensor_db.history")
devices_df.write.saveAsTable("sensor_db.devices")
history_df.write.saveAsTable("sensor_db.history")

devices_table_to_df = spark.sql("select * from sensor_db.devices")
history_table_to_df = spark.sql("select * from sensor_db.history")

# COMMAND ----------



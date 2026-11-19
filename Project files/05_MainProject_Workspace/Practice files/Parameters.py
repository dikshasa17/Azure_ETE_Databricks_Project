# Databricks notebook source
# Array of target dataset parameter dictionaries
data_sets = [
{"file_name": "orders"},
{"file_name": "customers"},
{"file_name": "products"},
]
# Send array to Databricks Workflows via taskValues
dbutils.jobs.taskValues.set(key="output_datasets", value=data_sets)

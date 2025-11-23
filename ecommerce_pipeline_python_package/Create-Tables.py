# Databricks notebook source
import os

base_path = "/Workspace/Shared/chatgpt"

folders = [
    "pei/bronze",
    "pei/silver",
    "pei/gold",
    "pei/utils",
    "jobs",
    "tests"
]

files = {
    "pei/__init__.py": "",
    "pei/bronze/__init__.py": "",
    "pei/bronze/customers_raw.py": "",
    "pei/bronze/products_raw.py": "",
    "pei/bronze/orders_raw.py": "",
    "pei/silver/__init__.py": "",
    "pei/silver/customers_enriched.py": "",
    "pei/silver/products_enriched.py": "",
    "pei/silver/orders_enriched.py": "",
    "pei/gold/__init__.py": "",
    "pei/gold/aggregate_profit.py": "",
    "pei/utils/__init__.py": "",
    "jobs/run_bronze.py": "",
    "jobs/run_silver.py": "",
    "jobs/run_gold.py": "",
    "tests/__init__.py": "",
    "tests/test_customers_enriched.py": "",
    "tests/test_products_enriched.py": "",
    "tests/test_orders_enriched.py": "",
    "tests/test_aggregate_profit.py": ""
}

for folder in folders:
    os.makedirs(os.path.join(base_path, folder), exist_ok=True)

for file_path, content in files.items():
    full_path = os.path.join(base_path, file_path)
    with open(full_path, "w") as f:
        f.write(content)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table workspace.default.watermark;
# MAGIC
# MAGIC DROP SCHEMA IF EXISTS workspace.raw CASCADE;
# MAGIC DROP SCHEMA IF EXISTS workspace.enriched CASCADE;
# MAGIC DROP SCHEMA IF EXISTS workspace.aggregate CASCADE;
# MAGIC DROP SCHEMA IF EXISTS workspace.quarantine CASCADE;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS workspace;
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.raw;
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.enriched;
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.aggregate;
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.quarantine;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS workspace.default.watermark (
# MAGIC     table_name STRING,
# MAGIC     column_name STRING,
# MAGIC     watermark_value STRING,
# MAGIC     updated_at TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC INSERT INTO workspace.default.watermark (table_name, column_name, watermark_value, updated_at) VALUES
# MAGIC ('workspace.raw.products','inserttimestamp','1900-01-01 00:00:00',current_timestamp()),
# MAGIC ('workspace.raw.customers','inserttimestamp','1900-01-01 00:00:00',current_timestamp()),
# MAGIC ('workspace.raw.orders','inserttimestamp','1900-01-01 00:00:00',current_timestamp()),
# MAGIC ('workspace.enriched.products','inserttimestamp','1900-01-01 00:00:00',current_timestamp()),
# MAGIC ('workspace.enriched.customers','inserttimestamp','1900-01-01 00:00:00',current_timestamp()),
# MAGIC ('workspace.enriched.orders','inserttimestamp','1900-01-01 00:00:00',current_timestamp());
# MAGIC
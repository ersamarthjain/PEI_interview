# Databricks notebook source
#Final code location = /Workspace/Shared/chatgpt/pei/enriched
#Old working code path = /Workspace/Users/er_samarth_jain@yahoo.com/PEI/ecommerce_data_pipeline/notebooks/enriched

# COMMAND ----------

import os

os.system("python /Workspace/Shared/chatgpt/jobs/run_bronze.py")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.raw.customers;

# COMMAND ----------

import sys
sys.path.append("/Workspacet")

from Shared.chatgpt.pei.bronze.customers_raw import load_raw_customers, write_raw_customers
from Shared.chatgpt.pei.utils import get_spark_session
from Shared.chatgpt.pei.utils.logger import get_logger
import argparse

def run_customers_pipeline(excel_path: str, table_name: str):
    """Run the customers raw pipeline"""
    logger = get_logger("Running Raw pipeline")
    spark = get_spark_session("Bronze_Customers_Pipeline")

    try:
        logger.info("Starting customers raw pipeline")
        df = load_raw_customers(spark, excel_path)
        write_raw_customers(df, table_name)
        logger.info("Customers raw pipeline completed successfully")
    except Exception as e:
        logger.error(f"Customers raw pipeline failed: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    customers_path = "/Volumes/workspace/default/pei/sales_data/Customer.xlsx"
    raw_customers_table = f"workspace.raw.customers"
    run_customers_pipeline(customers_path, raw_customers_table)

# COMMAND ----------

dbutils.fs.ls("/Volumes/workspace/default/pei/sales_data/Customer.xlsx")
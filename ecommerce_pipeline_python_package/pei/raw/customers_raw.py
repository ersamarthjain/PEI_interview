import pandas as pd
from pyspark.sql import functions as F
from pei.utils.logger import get_logger

logger = get_logger("raw.customers")


def load_raw_customers(spark, excel_path: str):
    """
    Reads the customers Excel file and loads it into a Spark DataFrame.
    Normalizes column names and adds inserttimestamp.
    """
    logger.info(f"Loading raw customers from: {excel_path}")

    try:
        pdf = pd.read_excel(excel_path, engine="openpyxl")
        pdf = pdf.astype(str)
        df = spark.createDataFrame(pdf)

        df = df.toDF(*[c.replace(" ", "").lower() for c in df.columns])
        df = df.withColumn("inserttimestamp", F.current_timestamp())
        logger.info(f"Excel loaded successfully")
    except FileNotFoundError:
        logger.error(f"File not found: {excel_path}")
        raise
    except Exception as e:
        logger.error(f"Failed reading Excel: {str(e)}")
        raise

    logger.info("Raw customers DataFrame prepared successfully")
    return df

def write_raw_customers(df, table_name: str):
    """
    Writes raw customer data into Delta in append mode.
    Includes full exception handling for Delta write failures.
    """
    logger.info(f"Writing raw customers to table: {table_name}")

    try:
        df.write.format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(table_name)

        logger.info("Write to raw layer successful")
    except Exception as e:
        logger.error(f"Failed writing raw customers to table {table_name}: {str(e)}")
        raise

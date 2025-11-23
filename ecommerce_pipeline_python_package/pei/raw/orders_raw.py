from pyspark.sql import functions as F
from pei.utils.logger import get_logger

logger = get_logger("raw.orders")

def load_raw_products(spark, orders_path: str):

    logger.info(f"Starting RAW Orders load from: {orders_path}")

    try:
        orders_raw = spark.read \
        .option("wholeFile", "true") \
        .option("multiline", "true") \
        .option("mode", "PERMISSIVE") \
        .option("columnNameOfCorruptRecord", "_corrupt_record") \
        .json(orders_path)

        orders_raw = orders_raw.toDF(*[c.replace(" ", "").lower() for c in orders_raw.columns])

        orders_raw = orders_raw.withColumn("inserttimestamp", F.current_timestamp())
        
        logger.info(f"Products CSV loaded successfully. Columns: {orders_raw.columns}")
        return orders_raw
    except FileNotFoundError:
        logger.error(f"File not found: {orders_path}")
        raise
    except Exception as e:
        logger.error(f"Failed to load Orders JSON: {str(e)}")
        raise

def write_raw_products(df, table_name: str):
    """
    Writes RAW Products into Delta with SCHEMA EVOLUTION ENABLED.
    """
    logger.info(f"Writing Orders RAW data to table: {table_name}")

    try:
        df.write.format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(table_name)

        logger.info("RAW Products successfully written to Delta.")

    except Exception as e:
        logger.error(f"Failed writing RAW Products to table {table_name}: {str(e)}")
        raise

from pyspark.sql import functions as F
from pei.utils.logger import get_logger

logger = get_logger("raw.products")

def load_raw_products(spark, products_path: str):

    logger.info(f"Starting RAW Products load from: {products_path}")

    try:
        products_raw = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .option("quote", '"')
            .option("escape", '"')
            .csv(products_path)
        )
        products_raw = products_raw.toDF(*[c.replace(" ", "").lower() for c in products_raw.columns])

        products_raw = products_raw.withColumn("inserttimestamp", F.current_timestamp())

        logger.info(f"Products CSV loaded successfully. Columns: {products_raw.columns}")
        return products_raw
    except FileNotFoundError:
        logger.error(f"File not found: {products_path}")
        raise
    except Exception as e:
        logger.error(f"Failed to load Products CSV: {str(e)}")
        raise

def write_raw_products(df, table_name: str):
    """
    Writes RAW Products into Delta with SCHEMA EVOLUTION ENABLED.
    """
    logger.info(f"Writing Products RAW data to table: {table_name}")

    try:
        df.write.format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(table_name)

        logger.info("RAW Products successfully written to Delta.")

    except Exception as e:
        logger.error(f"Failed writing RAW Products to table {table_name}: {str(e)}")
        raise

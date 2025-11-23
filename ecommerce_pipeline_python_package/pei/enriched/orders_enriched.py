# pei/silver/orders_enriched.py
from pyspark.sql import functions as F, DataFrame
from pyspark.sql.window import Window
from typing import Optional
from pei.utils.logger import get_logger

logger = get_logger("enriched.orders")

def transform_orders(df: DataFrame) -> DataFrame:
    """
    Transforms raw orders data with data quality checks and deduplication.

    Args:
        df: Input DataFrame containing raw orders data

    Returns:
        Transformed DataFrame with cleaned data and timestamps

    Raises:
        ValueError: If required columns are missing
    """
    logger.info("Starting orders transformation")

    required_cols = {
        "orderid", "orderdate", "shipdate", "shipmode",
        "customerid", "productid", "quantity", "price",
        "discount", "profit", "inserttimestamp"
    }

    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Date transformations
    df = (
        df.withColumn("orderdate", F.trim("orderdate"))
          .withColumn("shipdate", F.trim("shipdate"))
          .withColumn("orderdate", F.to_date("orderdate", "d/M/yyyy"))
          .withColumn("shipdate", F.to_date("shipdate", "d/M/yyyy"))
          .withColumn("orderyear", F.year("orderdate"))
    )

    # ID transformations
    df = (
        df.withColumn("orderid", F.upper(F.regexp_replace(F.trim("orderid"), r"\s+", "")))
          .withColumn("customerid", F.upper(F.regexp_replace(F.trim("customerid"), r"\s+", "")))
          .withColumn("productid", F.upper(F.regexp_replace(F.trim("productid"), r"\s+", "")))
    )

    # Basic validations
    df = df.filter(F.col("orderdate").isNotNull())
    df = df.filter(F.col("shipdate") >= F.col("orderdate"))
    df = df.filter(
        F.col("orderid").isNotNull() &
        F.col("customerid").isNotNull() &
        F.col("productid").isNotNull()
    )

    # Numeric field validations
    df = (
        df.withColumn("shipmode", F.initcap(F.trim("shipmode")))
          .withColumn("quantity", F.when(F.col("quantity") > 0, F.col("quantity")).otherwise(None).cast("int"))
          .withColumn("price", F.when(F.col("price") >= 0, F.col("price")).otherwise(None).cast("double"))
          .withColumn("discount", F.when(
              (F.col("discount") >= 0) & (F.col("discount") <= 1),
              F.col("discount")
          ).otherwise(None).cast("double"))
          .withColumn("profit", F.round(F.col("profit"), 2))
    )

    # Filter invalid numeric values
    df = df.filter(
        (F.col("quantity") > 0) &
        (F.col("price") > 0) &
        (F.col("discount") >= 0) & (F.col("discount") <= 1)
    )

    # Deduplication
    window = (
        Window.partitionBy("orderid", "productid", "customerid")
              .orderBy(F.col("orderdate").desc())
    )

    df = df.withColumn("rn", F.row_number().over(window)) \
           .filter("rn = 1").drop("rn")

    # Add timestamps
    df = (
        df.withColumn("inserttimestamp", F.current_timestamp())
          .withColumn("updatetimestamp", F.current_timestamp())
    )

    logger.info(f"Transformed {df.count()} orders records")
    return df

def write_orders_enriched(df: DataFrame, table_name: str, mode: str = "append") -> None:
    """
    Writes enriched orders data to Delta table.

    Args:
        df: DataFrame containing enriched orders data
        table_name: Target Delta table name
        mode: Write mode (append/overwrite)
    """
    from delta.tables import DeltaTable

    logger.info(f"Writing Orders Enriched to {table_name} in {mode} mode")

    try:
        (df.write
            .format("delta")
            .mode(mode)
            .option("mergeSchema", "true")
            .saveAsTable(table_name))

        logger.info(f"Orders enriched write successful - {df.count()} rows written")
    except Exception as e:
        logger.error(f"Failed to write orders enriched: {str(e)}")
        raise

def run_orders_enrichment(
    spark,
    raw_table: str = "workspace.raw.orders",
    enriched_table: str = "workspace.enriched.orders",
    watermark_table: str = "workspace.default.watermark"
) -> None:
    """
    Runs the complete orders enrichment pipeline.

    Args:
        spark: SparkSession
        raw_table: Source raw table name
        enriched_table: Target enriched table name
        watermark_table: Watermark table name
    """
    logger.info("Starting orders enrichment pipeline")

    try:
        # Get watermark
        wm_value = (
            spark.read.table(watermark_table)
            .filter(F.col("table_name") == raw_table)
            .select("watermark_value")
            .first()
        )

        # Load raw data
        raw_df = spark.read.table(raw_table)
        if wm_value:
            raw_df = raw_df.filter(F.col("inserttimestamp") > wm_value[0])

        if not raw_df.head(1):
            logger.info("No new orders records found")
            return

        # Transform data
        enriched_df = transform_orders(raw_df)

        # Write enriched data
        write_orders_enriched(enriched_df, enriched_table)

        # Update watermark
        max_ts = raw_df.agg({"inserttimestamp": "max"}).collect()[0][0]
        watermark_df = spark.createDataFrame(
            [(raw_table, max_ts)],
            ["table_name", "watermark_value"]
        )

        from delta.tables import DeltaTable
        delta_wm = DeltaTable.forName(spark, watermark_table)
        (delta_wm.alias("target")
            .merge(
                watermark_df.alias("source"),
                "target.table_name = source.table_name"
            )
            .whenMatchedUpdate(set={
                "watermark_value": "source.watermark_value",
                "updated_at": F.current_timestamp()
            })
            .execute())

        logger.info("Orders enrichment pipeline completed successfully")
    except Exception as e:
        logger.error(f"FAILED to process orders enriched: {str(e)}")
        raise
from pyspark.sql import functions as F, DataFrame
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from pei.utils.logger import get_logger

logger = get_logger("enriched.products")

def get_watermark(spark, watermark_table: str, source_table: str):
    """
    Retrieves watermark value for a given source table.

    Args:
        spark: SparkSession
        watermark_table: Watermark table name
        source_table: Source table name to get watermark for

    Returns:
        Watermark value or None if not found
    """
    try:
        return spark.read.table(watermark_table) \
            .filter(F.col("table_name") == source_table) \
            .select("watermark_value") \
            .first()
    except Exception as e:
        logger.error(f"Error retrieving watermark: {str(e)}")
        raise

def load_raw_products(spark, raw_table: str, watermark_value) -> DataFrame:
    """
    Loads raw products data with optional watermark filtering.

    Args:
        spark: SparkSession
        raw_table: Raw products table name
        watermark_value: Watermark value to filter by

    Returns:
        DataFrame with raw products data
    """
    try:
        if watermark_value:
            return spark.read.table(raw_table) \
                .filter(F.col("inserttimestamp") > watermark_value[0]) \
                .select('productid', 'category', 'subcategory', 'inserttimestamp')
        return spark.read.table(raw_table) \
            .select('productid', 'category', 'subcategory', 'inserttimestamp')
    except Exception as e:
        logger.error(f"Error loading raw products: {str(e)}")
        raise

def transform_products(raw_df: DataFrame) -> DataFrame:
    """
    Transforms raw products data with data quality checks and deduplication.

    Args:
        raw_df: Input DataFrame containing raw products data

    Returns:
        Transformed DataFrame with cleaned data and timestamps
    """
    logger.info("Starting products transformation")

    if not raw_df or not raw_df.head(1):
        logger.info("No new products records found")
        return raw_df.sparkSession.createDataFrame([], raw_df.schema)

    # Data Quality Checks
    dq_df = raw_df.filter(
        F.col("productid").isNotNull() &
        F.col("category").isNotNull() &
        F.col("subcategory").isNotNull()
    )

    # Data Cleaning
    dq_df = dq_df \
        .withColumn("productid", F.upper(F.trim("productid"))) \
        .withColumn("productid", F.regexp_replace("productid", r"\s+", "")) \
        .withColumn("category", F.when(F.col("category") == "", None).otherwise(F.col("category"))) \
        .withColumn("category", F.initcap(F.trim(F.col("category")))) \
        .withColumn("subcategory", F.when(F.col("subcategory") == "", None).otherwise(F.col("subcategory"))) \
        .withColumn("subcategory", F.initcap(F.trim(F.col("subcategory")))) \
        .fillna({"category": "Unknown", "subcategory": "Unknown"}) \
        .withColumn("inserttimestamp", F.current_timestamp()) \
        .withColumn("updatetimestamp", F.current_timestamp())

    # Deduplication
    w = Window.partitionBy("productid").orderBy(F.col("inserttimestamp").desc())
    dq_df = dq_df \
        .withColumn("rn", F.row_number().over(w)) \
        .filter(F.col("rn") == 1) \
        .drop("rn")

    dq_df = dq_df.withColumn("inserttimestamp", F.current_timestamp()) \
        .withColumn("updatetimestamp", F.current_timestamp())

    logger.info(f"Transformed {dq_df.count()} products records")
    return dq_df

def merge_products(spark, enriched_df: DataFrame, enriched_table: str) -> None:
    """
    Merges enriched products data into the target table.

    Args:
        spark: SparkSession
        enriched_df: DataFrame containing enriched products data
        enriched_table: Target Delta table name
    """
    logger.info(f"Merging into enriched products table: {enriched_table}")

    try:
        if spark.catalog.tableExists(enriched_table):
            delta_table = DeltaTable.forName(spark, enriched_table)
            delta_table.alias("target").merge(
                enriched_df.alias("source"),
                "target.productid = source.productid") \
            .whenMatchedUpdate(set={
                "category": "source.category",
                "subcategory": "source.subcategory",
                "updatetimestamp": "source.updatetimestamp"
            }) \
            .whenNotMatchedInsertAll().execute()
        else:
            enriched_df.write \
                .format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .partitionBy('category', 'subcategory') \
                .saveAsTable(enriched_table)
    except Exception as e:
        logger.error(f"Error merging products: {str(e)}")
        raise

def update_watermark(spark, watermark_table: str, source_table: str, max_ts) -> None:
    """
    Updates the watermark table with the latest timestamp.

    Args:
        spark: SparkSession
        watermark_table: Watermark table name
        source_table: Source table name
        max_ts: Maximum timestamp value
    """
    try:
        watermark_df = spark.createDataFrame(
            [(source_table, max_ts)],
            ["table_name", "watermark_value"]
        )

        watermark_delta = DeltaTable.forName(spark, watermark_table)
        watermark_delta.alias("target").merge(
            watermark_df.alias("source"),
            "target.table_name = source.table_name") \
        .whenMatchedUpdate(set={
            "watermark_value": "source.watermark_value",
            "updated_at": F.current_timestamp()
        }).execute()
    except Exception as e:
        logger.error(f"Error updating watermark: {str(e)}")
        raise

def quarantine_failed_records(spark, raw_df: DataFrame, enriched_df: DataFrame) -> None:
    """
    Identifies and quarantines failed records.

    Args:
        spark: SparkSession
        raw_df: Original raw DataFrame
        enriched_df: Enriched DataFrame
    """
    try:
        raw_df = raw_df.withColumn("updatetimestamp", F.lit(None).cast("timestamp"))
        failed_df = raw_df.subtract(enriched_df)

        if failed_df.count() > 0:
            failed_df.write.format("delta") \
                .mode("append") \
                .saveAsTable("workspace.quarantine.products")
            logger.info(f"Quarantined {failed_df.count()} failed records")
    except Exception as e:
        logger.error(f"Error quarantining failed records: {str(e)}")
        raise

def run_products_enrichment(spark, raw_table: str = "workspace.raw.products",
                          enriched_table: str = "workspace.enriched.products",
                          watermark_table: str = "workspace.default.watermark") -> None:
    """
    Runs the complete products enrichment pipeline.

    Args:
        spark: SparkSession
        raw_table: Source raw table name
        enriched_table: Target enriched table name
        watermark_table: Watermark table name
    """
    logger.info("Starting products enrichment pipeline")

    try:
        # Get watermark
        watermark_ts = get_watermark(spark, watermark_table, raw_table)

        # Load raw data
        raw_products_df = load_raw_products(spark, raw_table, watermark_ts)

        if not raw_products_df.head(1):
            logger.info("No new products records found")
            return

        # Get max timestamp
        max_ts = raw_products_df.agg({"inserttimestamp": "max"}).collect()[0][0]

        # Transform data
        enriched_df = transform_products(raw_products_df)

        # Merge into enriched table
        merge_products(spark, enriched_df, enriched_table)

        # Update watermark
        update_watermark(spark, watermark_table, raw_table, max_ts)

        # Quarantine failed records
        quarantine_failed_records(spark, raw_products_df, enriched_df)

        logger.info("Products enrichment pipeline completed successfully")
    except Exception as e:
        logger.error(f"FAILED to process products enriched: {str(e)}")
        raise
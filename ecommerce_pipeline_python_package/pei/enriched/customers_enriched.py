from pyspark.sql import functions as F, DataFrame
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from pei.utils.logger import get_logger

logger = get_logger("enriched.customers")

def transform_customers(raw_df: DataFrame) -> DataFrame:
    """
    Transforms raw customer data with data quality checks and deduplication.

    Args:
        raw_df: Input DataFrame containing raw customer data

    Returns:
        Transformed DataFrame with cleaned data and timestamps
    """
    logger.info("Starting transform_customers()")

    if raw_df is None or raw_df.head(1) == []:
        logger.info("RAW DF empty. Returning empty DF.")
        return raw_df.sparkSession.createDataFrame([], raw_df.schema)

    required = {"customerid", "customername", "country", "inserttimestamp"}
    missing = required - set(raw_df.columns)

    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    dq_df = raw_df.filter(
        F.col("customerid").isNotNull() &
        F.col("customername").isNotNull() &
        F.col("country").isNotNull()
    )

    dq_df = dq_df.withColumn("customerid", F.upper(F.trim("customerid")))
    dq_df = dq_df.withColumn("customerid", F.regexp_replace("customerid", r"\s+", ""))

    dq_df = dq_df.withColumn(
        "customername", F.regexp_replace("customername", r"[^A-Za-z' ]+", "")
    )
    dq_df = dq_df.withColumn("customername", F.regexp_replace("customername", r"\s+", " "))

    dq_df = dq_df.withColumn("country", F.initcap(F.trim("country")))

    w = Window.partitionBy("customerid").orderBy(F.col("inserttimestamp").desc())
    dq_df = dq_df.withColumn("rn", F.row_number().over(w)) \
                 .filter("rn = 1") \
                 .drop("rn")

    dq_df = dq_df.withColumn("inserttimestamp", F.current_timestamp()) \
                 .withColumn("updatetimestamp", F.current_timestamp())

    logger.info("transform_customers() completed")
    return dq_df

def merge_customers(spark, enriched_df: DataFrame, enriched_table: str) -> None:
    """
    Merges enriched customer data into the target table.

    Args:
        spark: SparkSession
        enriched_df: DataFrame containing enriched customer data
        enriched_table: Target Delta table name
    """
    logger.info(f"Merging into enriched table: {enriched_table}")

    if spark.catalog.tableExists(enriched_table):
        delta_table = DeltaTable.forName(spark, enriched_table)
        (
            delta_table.alias("target")
            .merge(
                enriched_df.alias("source"),
                "target.customerid = source.customerid"
            )
            .whenMatchedUpdate(set={
                "customername": "source.customername",
                "country": "source.country",
                "updatetimestamp": "source.updatetimestamp"
            })
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        enriched_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .partitionBy("country") \
            .saveAsTable(enriched_table)

    logger.info("Merge complete.")

def run_customers_enrichment(spark, raw_table: str = "workspace.raw.customers",
                           enriched_table: str = "workspace.enriched.customers",
                           watermark_table: str = "workspace.default.watermark") -> None:
    """
    Runs the complete customers enrichment pipeline.

    Args:
        spark: SparkSession
        raw_table: Source raw table name
        enriched_table: Target enriched table name
        watermark_table: Watermark table name
    """
    logger.info("Starting full customers enrichment pipeline...")

    try:
        wm_value = (
            spark.read.table(watermark_table)
            .filter(F.col("table_name") == raw_table)
            .select("watermark_value")
            .first()
        )

        if wm_value:
            raw_df = (
                spark.read.table(raw_table)
                .filter(F.col("inserttimestamp") > wm_value[0])
                .select("customerid", "customername", "country", "inserttimestamp")
            )
        else:
            raw_df = spark.read.table(raw_table) \
                .select("customerid", "customername", "country", "inserttimestamp")

        if not raw_df.head(1):
            logger.info("No new RAW records found.")
            return

        max_ts = raw_df.agg({"inserttimestamp": "max"}).collect()[0][0]

        enriched_df = transform_customers(raw_df)

        merge_customers(spark, enriched_df, enriched_table)

        watermark_df = spark.createDataFrame(
            [(raw_table, max_ts)],
            ["table_name", "watermark_value"]
        )

        delta_wm = DeltaTable.forName(spark, watermark_table)
        (
            delta_wm.alias("target")
            .merge(
                watermark_df.alias("source"),
                "target.table_name = source.table_name"
            )
            .whenMatchedUpdate(set={
                "watermark_value": "source.watermark_value",
                "updated_at": F.current_timestamp()
            })
            .execute()
        )

        raw_df_with_update_col = raw_df.withColumn("updatetimestamp", F.lit(None).cast("timestamp"))
        failed_df = raw_df_with_update_col.subtract(enriched_df)

        if failed_df.count() > 0:
            failed_df.write.format("delta") \
                .mode("append") \
                .saveAsTable("workspace.quarantine.customers")

    except Exception as e:
        logger.error(f"FAILED to process customers enriched: {str(e)}")
        raise

    logger.info("Customers enrichment pipeline complete.")
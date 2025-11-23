from pyspark.sql import functions as F, DataFrame
from delta.tables import DeltaTable
from datetime import datetime
from typing import Dict, Optional, Tuple
from pei.utils.logger import get_logger

logger = get_logger("aggregate.profit")

def safe_max_ts(df: DataFrame, col: str = "updatetimestamp") -> Optional[datetime]:
    """
    Safely gets the maximum timestamp from a DataFrame column.

    Args:
        df: Input DataFrame
        col: Column name to get max timestamp from

    Returns:
        Maximum timestamp or None if no data
    """
    row = df.agg(F.max(col).alias("maxts")).first()
    return row["maxts"] if row and row["maxts"] is not None else None

def rows_since(df: DataFrame, ts_col: str = "updatetimestamp", wm_value: Optional[datetime] = None) -> DataFrame:
    """
    Filters DataFrame to get rows since a watermark timestamp.

    Args:
        df: Input DataFrame
        ts_col: Timestamp column name
        wm_value: Watermark value to filter by

    Returns:
        Filtered DataFrame with rows since watermark
    """
    return df.filter(F.col(ts_col) > wm_value) if wm_value is not None else df

def get_watermarks(spark, watermark_table: str, table_names: list) -> Dict[str, datetime]:
    """
    Retrieves watermark values for multiple tables.

    Args:
        spark: SparkSession
        watermark_table: Watermark table name
        table_names: List of table names to get watermarks for

    Returns:
        Dictionary of table names to watermark values
    """
    try:
        wm_rows = (
            spark.read.table(watermark_table)
            .filter(F.col("table_name").isin(table_names))
            .select("table_name", "watermark_value")
            .collect()
        )
        return {row["table_name"]: row["watermark_value"] for row in wm_rows}
    except Exception as e:
        logger.error(f"Error retrieving watermarks: {str(e)}")
        raise

def load_enriched_tables(spark, tables: Dict[str, str]) -> Dict[str, DataFrame]:
    """
    Loads enriched tables with minimal required columns.

    Args:
        spark: SparkSession
        tables: Dictionary of table names to column lists

    Returns:
        Dictionary of table names to DataFrames
    """
    try:
        return {
            table_name: spark.read.table(table_name).select(*columns)
            for table_name, columns in tables.items()
        }
    except Exception as e:
        logger.error(f"Error loading enriched tables: {str(e)}")
        raise

def full_recompute(
    orders_df: DataFrame,
    products_df: DataFrame,
    customers_df: DataFrame,
    aggregate_table: str
) -> None:
    """
    Performs a full recompute of the aggregate table.

    Args:
        orders_df: Enriched orders DataFrame
        products_df: Enriched products DataFrame
        customers_df: Enriched customers DataFrame
        aggregate_table: Target aggregate table name
    """
    try:
        products_full_df = products_df.select("productid", "category", "subcategory")
        customers_full_df = customers_df.select("customerid", "customername")

        full_recompute_df = (
            orders_df
            .join(products_full_df, "productid", "left")
            .join(customers_full_df, "customerid", "left")
        )

        # Ensure types are stable before groupBy
        full_recompute_df = (
            full_recompute_df
            .withColumn("category", F.col("category").cast("string"))
            .withColumn("subcategory", F.col("subcategory").cast("string"))
            .withColumn("customername", F.col("customername").cast("string"))
            .withColumn("profit", F.col("profit").cast("double"))
            .withColumn("orderyear", F.col("orderyear").cast("int"))
        )

        recomputed_agg_df = (
            full_recompute_df
            .groupBy("orderyear", "category", "subcategory", "customerid", "customername")
            .agg(F.sum("profit").alias("totalprofit"))
            .withColumn("inserttimestamp", F.current_timestamp())
            .withColumn("updatetimestamp", F.current_timestamp())
        )

        # Overwrite aggregate table (safe full refresh)
        (
            recomputed_agg_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("orderyear")
            .saveAsTable(aggregate_table)
        )
    except Exception as e:
        logger.error(f"Error during full recompute: {str(e)}")
        raise

def update_customer_names(
    spark,
    customers_changed: DataFrame,
    aggregate_table: str
) -> None:
    """
    Updates customer names in the aggregate table.

    Args:
        spark: SparkSession
        customers_changed: DataFrame with changed customers
        aggregate_table: Target aggregate table name
    """
    try:
        if not spark.catalog.tableExists(aggregate_table):
            return

        cust_change = customers_changed.select("customerid", "customername", "updatetimestamp")
        agg = spark.read.table(aggregate_table)

        # Find aggregate rows for changed customers
        updates_df = (
            agg.alias("a")
            .join(cust_change.alias("c"), "customerid", "inner")
            .select(
                F.col("a.orderyear").alias("orderyear"),
                F.col("a.category").alias("category"),
                F.col("a.subcategory").alias("subcategory"),
                F.col("a.customerid").alias("customerid"),
                F.col("c.customername").alias("customername"),
                F.current_timestamp().alias("updatetimestamp")
            )
            .dropDuplicates(["orderyear", "category", "subcategory", "customerid"])
        )

        target = DeltaTable.forName(spark, aggregate_table)
        merge_cond = """
            target.orderyear = source.orderyear AND
            target.category = source.category AND
            target.subcategory = source.subcategory AND
            target.customerid = source.customerid
        """

        target.alias("target").merge(
            updates_df.alias("source"), merge_cond
        ).whenMatchedUpdate(set={
            "customername": "source.customername",
            "updatetimestamp": "source.updatetimestamp"
        }).execute()
    except Exception as e:
        logger.error(f"Error updating customer names: {str(e)}")
        raise

def incremental_aggregate(
    spark,
    orders_changed: DataFrame,
    products_df: DataFrame,
    customers_df: DataFrame,
    aggregate_table: str
) -> None:
    """
    Performs incremental aggregation of changed orders.

    Args:
        spark: SparkSession
        orders_changed: DataFrame with changed orders
        products_df: Enriched products DataFrame
        customers_df: Enriched customers DataFrame
        aggregate_table: Target aggregate table name
    """
    try:
        products_df = products_df.select("productid", "category", "subcategory")
        customers_df = customers_df.select("customerid", "customername")

        enriched_df = (
            orders_changed
            .join(products_df, "productid", "left")
            .join(customers_df, "customerid", "left")
        ).withColumn("category", F.col("category").cast("string")) \
         .withColumn("subcategory", F.col("subcategory").cast("string")) \
         .withColumn("customername", F.col("customername").cast("string")) \
         .withColumn("profit", F.col("profit").cast("double")) \
         .withColumn("orderyear", F.col("orderyear").cast("int"))

        agg_df = (
            enriched_df
            .groupBy("orderyear", "category", "subcategory", "customerid", "customername")
            .agg(F.sum("profit").alias("totalprofit"))
            .withColumn("inserttimestamp", F.current_timestamp())
            .withColumn("updatetimestamp", F.current_timestamp())
        )

        if spark.catalog.tableExists(aggregate_table):
            target = DeltaTable.forName(spark, aggregate_table)
            merge_cond = """
                target.orderyear = source.orderyear AND
                target.category = source.category AND
                target.subcategory = source.subcategory AND
                target.customerid = source.customerid
            """
            target.alias("target").merge(
                agg_df.alias("source"),
                merge_cond
            ).whenMatchedUpdate(set={
                "totalprofit": "target.totalprofit + source.totalprofit",
                "updatetimestamp": "source.updatetimestamp"
            }).whenNotMatchedInsertAll().execute()
        else:
            (agg_df.write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .partitionBy("orderyear")
                .saveAsTable(aggregate_table)
            )
    except Exception as e:
        logger.error(f"Error during incremental aggregation: {str(e)}")
        raise

def update_watermarks(
    spark,
    watermark_table: str,
    table_updates: Dict[str, Tuple[str, datetime]]
) -> None:
    """
    Updates watermarks for multiple tables.

    Args:
        spark: SparkSession
        watermark_table: Watermark table name
        table_updates: Dictionary of table names to (column_name, max_ts) tuples
    """
    try:
        watermark_rows = [
            (table_name, column_name, max_ts, F.current_timestamp())
            for table_name, (column_name, max_ts) in table_updates.items()
        ]
        watermark_df = spark.createDataFrame(
            watermark_rows,
            ["table_name", "column_name", "watermark_value", "updated_at"]
        )

        watermark_delta = DeltaTable.forName(spark, watermark_table)
        watermark_delta.alias("target").merge(
            watermark_df.alias("source"),
            "target.table_name = source.table_name"
        ).whenMatchedUpdate(set={
            "watermark_value": "source.watermark_value",
            "updated_at": "source.updated_at"
        }).whenNotMatchedInsertAll().execute()
    except Exception as e:
        logger.error(f"Error updating watermarks: {str(e)}")
        raise

def run_aggregate_profit(
    spark,
    enriched_orders_table: str = "workspace.enriched.orders",
    enriched_products_table: str = "workspace.enriched.products",
    enriched_customers_table: str = "workspace.enriched.customers",
    aggregate_profits_table: str = "workspace.aggregate.profits",
    watermark_table: str = "workspace.default.watermark"
) -> None:
    """
    Runs the complete aggregate profit pipeline.

    Args:
        spark: SparkSession
        enriched_orders_table: Enriched orders table name
        enriched_products_table: Enriched products table name
        enriched_customers_table: Enriched customers table name
        aggregate_profits_table: Target aggregate table name
        watermark_table: Watermark table name
    """
    logger.info("Starting aggregate profit pipeline")

    try:
        # Define tables and their required columns
        tables = {
            enriched_orders_table: ["orderid", "orderyear", "customerid", "productid", "profit", "updatetimestamp"],
            enriched_products_table: ["productid", "category", "subcategory", "updatetimestamp"],
            enriched_customers_table: ["customerid", "customername", "country", "updatetimestamp"]
        }

        # Get watermarks for all tables
        wm_dict = get_watermarks(spark, watermark_table, list(tables.keys()))
        orders_wm = wm_dict.get(enriched_orders_table)
        products_wm = wm_dict.get(enriched_products_table)
        customers_wm = wm_dict.get(enriched_customers_table)

        # Load all enriched tables
        enriched_dfs = load_enriched_tables(spark, tables)
        full_enriched_orders_df = enriched_dfs[enriched_orders_table]
        full_enriched_products_df = enriched_dfs[enriched_products_table]
        full_enriched_customers_df = enriched_dfs[enriched_customers_table]

        # Determine changed rows
        orders_changed = rows_since(full_enriched_orders_df, "updatetimestamp", orders_wm)
        products_changed = rows_since(full_enriched_products_df, "updatetimestamp", products_wm)
        customers_changed = rows_since(full_enriched_customers_df, "updatetimestamp", customers_wm)

        # Priority: products -> customers -> orders
        if products_changed.head(1):
            logger.info("Products changed - performing full recompute")
            full_recompute(
                full_enriched_orders_df,
                full_enriched_products_df,
                full_enriched_customers_df,
                aggregate_profits_table
            )

            # Update watermarks for all tables
            table_updates = {
                enriched_orders_table: ("updatetimestamp", safe_max_ts(full_enriched_orders_df)),
                enriched_products_table: ("updatetimestamp", safe_max_ts(full_enriched_products_df)),
                enriched_customers_table: ("updatetimestamp", safe_max_ts(full_enriched_customers_df))
            }
            update_watermarks(spark, watermark_table, table_updates)

        elif customers_changed.head(1):
            logger.info("Customers changed - updating customer names")
            update_customer_names(spark, customers_changed, aggregate_profits_table)

            # Update customer watermark
            table_updates = {
                enriched_customers_table: ("updatetimestamp", safe_max_ts(customers_changed))
            }
            update_watermarks(spark, watermark_table, table_updates)

        elif orders_changed.head(1):
            logger.info("Orders changed - performing incremental aggregation")
            incremental_aggregate(
                spark,
                orders_changed,
                full_enriched_products_df,
                full_enriched_customers_df,
                aggregate_profits_table
            )

            # Update orders watermark
            table_updates = {
                enriched_orders_table: ("updatetimestamp", safe_max_ts(orders_changed))
            }
            update_watermarks(spark, watermark_table, table_updates)

        else:
            logger.info("No changes detected in products/customers/orders")

    except Exception as e:
        logger.error(f"FAILED to process aggregate profit: {str(e)}")
        raise
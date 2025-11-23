# jobs/run_silver.py
from pei.silver.customers_enriched import run_customers_enrichment
from pei.silver.products_enriched import run_products_enrichment
from pei.silver.orders_enriched import run_orders_enrichment
from pei.utils import get_spark_session, setup_logging
import argparse

def run_customers_enrichment_pipeline(raw_table: str, enriched_table: str, watermark_table: str):
    """Run the customers enrichment pipeline"""
    logger = setup_logging()
    spark = get_spark_session("Silver_Customers_Pipeline")

    try:
        logger.info("Starting customers enrichment pipeline")
        run_customers_enrichment(spark, raw_table, enriched_table, watermark_table)
        logger.info("Customers enrichment pipeline completed successfully")
    except Exception as e:
        logger.error(f"Customers enrichment pipeline failed: {str(e)}")
        raise
    finally:
        spark.stop()

def run_products_enrichment_pipeline(raw_table: str, enriched_table: str, watermark_table: str):
    """Run the products enrichment pipeline"""
    logger = setup_logging()
    spark = get_spark_session("Silver_Products_Pipeline")

    try:
        logger.info("Starting products enrichment pipeline")
        run_products_enrichment(spark, raw_table, enriched_table, watermark_table)
        logger.info("Products enrichment pipeline completed successfully")
    except Exception as e:
        logger.error(f"Products enrichment pipeline failed: {str(e)}")
        raise
    finally:
        spark.stop()

def run_orders_enrichment_pipeline(raw_table: str, enriched_table: str, watermark_table: str):
    """Run the orders enrichment pipeline"""
    logger = setup_logging()
    spark = get_spark_session("Silver_Orders_Pipeline")

    try:
        logger.info("Starting orders enrichment pipeline")
        run_orders_enrichment(spark, raw_table, enriched_table, watermark_table)
        logger.info("Orders enrichment pipeline completed successfully")
    except Exception as e:
        logger.error(f"Orders enrichment pipeline failed: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Silver Enrichment Pipelines")
    parser.add_argument("--entity", required=True, choices=["customers", "products", "orders"],
                       help="Entity to process (customers, products, or orders)")
    parser.add_argument("--raw_table", help="Source raw table name")
    parser.add_argument("--enriched_table", help="Target enriched table name")
    parser.add_argument("--watermark_table", help="Watermark table name")
    args = parser.parse_args()

    if args.entity == "customers":
        raw_table = args.raw_table or "workspace.raw.customers"
        enriched_table = args.enriched_table or "workspace.enriched.customers"
        watermark_table = args.watermark_table or "workspace.default.watermark"
        run_customers_enrichment_pipeline(raw_table, enriched_table, watermark_table)
    elif args.entity == "products":
        raw_table = args.raw_table or "workspace.raw.products"
        enriched_table = args.enriched_table or "workspace.enriched.products"
        watermark_table = args.watermark_table or "workspace.default.watermark"
        run_products_enrichment_pipeline(raw_table, enriched_table, watermark_table)
    elif args.entity == "orders":
        raw_table = args.raw_table or "workspace.raw.orders"
        enriched_table = args.enriched_table or "workspace.enriched.orders"
        watermark_table = args.watermark_table or "workspace.default.watermark"
        run_orders_enrichment_pipeline(raw_table, enriched_table, watermark_table)
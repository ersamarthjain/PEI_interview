from pei.gold.aggregate_profit import run_aggregate_profit
from pei.utils import get_spark_session, setup_logging
import argparse

def run_aggregate_profit_pipeline(
    enriched_orders_table: str,
    enriched_products_table: str,
    enriched_customers_table: str,
    aggregate_profits_table: str,
    watermark_table: str
):
    """Run the aggregate profit pipeline"""
    logger = setup_logging()
    spark = get_spark_session("Gold_Aggregate_Profit_Pipeline")

    try:
        logger.info("Starting aggregate profit pipeline")
        run_aggregate_profit(
            spark,
            enriched_orders_table,
            enriched_products_table,
            enriched_customers_table,
            aggregate_profits_table,
            watermark_table
        )
        logger.info("Aggregate profit pipeline completed successfully")
    except Exception as e:
        logger.error(f"Aggregate profit pipeline failed: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    run_aggregate_profit_pipeline(
        workspace.enriched.orders,
        workspace.enriched.products,
        workspace.enriched.customers,
        workspace.aggregate.profits,
        workspace.default.watermark
    )
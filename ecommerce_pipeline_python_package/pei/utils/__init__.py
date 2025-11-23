# pei/utils/__init__.py
from pyspark.sql import SparkSession
#import logging

def get_spark_session(app_name="PEI_Pipeline"):
    """Create and return a Spark session"""
    return SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()

# def setup_logging():
#     """Setup basic logging configuration"""
#     logging.basicConfig(
#         level=logging.INFO,
#         format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
#     )
#     return logging.getLogger(__name__)
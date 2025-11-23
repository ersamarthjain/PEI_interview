import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql import Row
from delta.tables import DeltaTable
from pei.enriched.customers_enriched import transform_customers, merge_customers, run_customers_enrichment
import tempfile
import os
from datetime import datetime, timedelta

@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession.builder \
        .appName("test_customers_enriched") \
        .master("local[1]") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.sql.warehouse.dir", tempfile.mkdtemp()) \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture
def sample_raw_data(spark_session):
    schema = StructType([
        StructField("customerid", StringType(), True),
        StructField("customername", StringType(), True),
        StructField("country", StringType(), True),
        StructField("inserttimestamp", TimestampType(), True)
    ])

    data = [
        ("cust1", "John Doe", "usa", datetime.now() - timedelta(days=2)),
        ("cust2", "Jane Smith", "UK", datetime.now() - timedelta(days=1)),
        ("cust3", "Bob Johnson", "Canada", datetime.now())
    ]

    return spark_session.createDataFrame(data, schema)

@pytest.fixture
def empty_raw_data(spark_session):
    schema = StructType([
        StructField("customerid", StringType(), True),
        StructField("customername", StringType(), True),
        StructField("country", StringType(), True),
        StructField("inserttimestamp", TimestampType(), True)
    ])
    return spark_session.createDataFrame([], schema)

def test_transform_customers_success(spark_session, sample_raw_data):
    # Test successful transformation
    result = transform_customers(sample_raw_data)

    # Verify column transformations
    assert result.count() == 3
    assert set(result.columns) == {"customerid", "customername", "country", "inserttimestamp", "updatetimestamp"}

    # Verify data cleaning
    cust1 = result.filter("customerid = 'CUST1'").first()
    assert cust1["customername"] == "John Doe"
    assert cust1["country"] == "Usa"

def test_transform_customers_empty_input(spark_session, empty_raw_data):
    # Test empty input
    result = transform_customers(empty_raw_data)
    assert result.count() == 0

def test_transform_customers_missing_columns(spark_session):
    # Test missing required columns
    schema = StructType([
        StructField("customerid", StringType(), True),
        StructField("customername", StringType(), True)
    ])
    df = spark_session.createDataFrame([], schema)

    with pytest.raises(ValueError, match="Missing required columns"):
        transform_customers(df)

def test_transform_customers_null_values(spark_session):
    # Test null handling
    schema = StructType([
        StructField("customerid", StringType(), True),
        StructField("customername", StringType(), True),
        StructField("country", StringType(), True),
        StructField("inserttimestamp", TimestampType(), True)
    ])

    data = [
        ("cust1", None, "usa", datetime.now()),
        (None, "Jane Smith", "UK", datetime.now()),
        ("cust3", "Bob Johnson", None, datetime.now())
    ]

    df = spark_session.createDataFrame(data, schema)
    result = transform_customers(df)
    assert result.count() == 0  # All rows should be filtered out

def test_transform_customers_deduplication(spark_session):
    # Test deduplication
    schema = StructType([
        StructField("customerid", StringType(), True),
        StructField("customername", StringType(), True),
        StructField("country", StringType(), True),
        StructField("inserttimestamp", TimestampType(), True)
    ])

    data = [
        ("cust1", "John Doe", "usa", datetime.now() - timedelta(hours=2)),
        ("cust1", "John Doe Updated", "usa", datetime.now() - timedelta(hours=1)),
        ("cust2", "Jane Smith", "UK", datetime.now())
    ]

    df = spark_session.createDataFrame(data, schema)
    result = transform_customers(df)

    # Should keep only the latest record for each customer
    assert result.count() == 2
    cust1 = result.filter("customerid = 'CUST1'").first()
    assert cust1["customername"] == "John Doe Updated"

def test_merge_customers_new_table(spark_session, sample_raw_data):
    # Test merge into new table
    test_table = "test_enriched_customers_new"

    enriched_df = transform_customers(sample_raw_data)
    merge_customers(spark_session, enriched_df, test_table)

    # Verify table was created
    result = spark_session.table(test_table)
    assert result.count() == 3

    # Clean up
    spark_session.sql(f"DROP TABLE IF EXISTS {test_table}")

def test_merge_customers_existing_table(spark_session, sample_raw_data):
    # Test merge into existing table
    test_table = "test_enriched_customers_existing"

    # First create the table
    enriched_df = transform_customers(sample_raw_data)
    merge_customers(spark_session, enriched_df, test_table)

    # Now update one record and add a new one
    updated_data = [
        ("cust1", "John Doe Updated", "USA", datetime.now()),
        ("cust4", "New Customer", "France", datetime.now())
    ]
    updated_df = spark_session.createDataFrame(updated_data, sample_raw_data.schema)
    updated_enriched = transform_customers(updated_df)

    merge_customers(spark_session, updated_enriched, test_table)

    # Verify updates
    result = spark_session.table(test_table)
    assert result.count() == 4
    cust1 = result.filter("customerid = 'CUST1'").first()
    assert cust1["customername"] == "John Doe Updated"

    # Clean up
    spark_session.sql(f"DROP TABLE IF EXISTS {test_table}")

def test_run_customers_enrichment(spark_session):
    # Setup test tables
    raw_table = "test_raw_customers"
    enriched_table = "test_enriched_customers"
    watermark_table = "test_watermark"

    # Create watermark table
    watermark_schema = StructType([
        StructField("table_name", StringType(), True),
        StructField("watermark_value", TimestampType(), True),
        StructField("updated_at", TimestampType(), True)
    ])

    watermark_data = [(raw_table, datetime.now() - timedelta(days=3), datetime.now())]
    watermark_df = spark_session.createDataFrame(watermark_data, watermark_schema)
    watermark_df.write.format("delta").saveAsTable(watermark_table)

    # Create raw data with some new records
    raw_schema = StructType([
        StructField("customerid", StringType(), True),
        StructField("customername", StringType(), True),
        StructField("country", StringType(), True),
        StructField("inserttimestamp", TimestampType(), True)
    ])

    raw_data = [
        ("cust1", "John Doe", "usa", datetime.now() - timedelta(days=2)),  # Old record
        ("cust2", "Jane Smith", "UK", datetime.now() - timedelta(days=1)),  # Old record
        ("cust3", "Bob Johnson", "Canada", datetime.now())  # New record
    ]
    raw_df = spark_session.createDataFrame(raw_data, raw_schema)
    raw_df.write.format("delta").saveAsTable(raw_table)

    # Run the pipeline
    run_customers_enrichment(spark_session, raw_table, enriched_table, watermark_table)

    # Verify results
    enriched_result = spark_session.table(enriched_table)
    assert enriched_result.count() == 1  # Only the new record should be processed

    watermark_result = spark_session.table(watermark_table)
    assert watermark_result.filter(f"table_name = '{raw_table}'").count() == 1

    # Clean up
    spark_session.sql(f"DROP TABLE IF EXISTS {raw_table}")
    spark_session.sql(f"DROP TABLE IF EXISTS {enriched_table}")
    spark_session.sql(f"DROP TABLE IF EXISTS {watermark_table}")
import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql import Row
from delta.tables import DeltaTable
from pei.silver.products_enriched import (
    get_watermark, load_raw_products, transform_products,
    merge_products, update_watermark, quarantine_failed_records,
    run_products_enrichment
)
import tempfile
import os
from datetime import datetime, timedelta

@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession.builder \
        .appName("test_products_enriched") \
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
def sample_raw_products(spark_session):
    schema = StructType([
        StructField("productid", StringType(), True),
        StructField("category", StringType(), True),
        StructField("subcategory", StringType(), True),
        StructField("inserttimestamp", TimestampType(), True)
    ])

    data = [
        ("prod1", "Electronics", "Laptops", datetime.now() - timedelta(days=2)),
        ("prod2", "Furniture", "Chairs", datetime.now() - timedelta(days=1)),
        ("prod3", "Clothing", "Shirts", datetime.now())
    ]

    return spark_session.createDataFrame(data, schema)

@pytest.fixture
def empty_raw_products(spark_session):
    schema = StructType([
        StructField("productid", StringType(), True),
        StructField("category", StringType(), True),
        StructField("subcategory", StringType(), True),
        StructField("inserttimestamp", TimestampType(), True)
    ])
    return spark_session.createDataFrame([], schema)

def test_get_watermark(spark_session):
    # Setup test watermark table
    watermark_table = "test_watermark"
    watermark_schema = StructType([
        StructField("table_name", StringType(), True),
        StructField("watermark_value", TimestampType(), True),
        StructField("updated_at", TimestampType(), True)
    ])

    watermark_data = [("workspace.raw.products", datetime.now() - timedelta(days=1), datetime.now())]
    watermark_df = spark_session.createDataFrame(watermark_data, watermark_schema)
    watermark_df.write.format("delta").saveAsTable(watermark_table)

    # Test getting watermark
    result = get_watermark(spark_session, watermark_table, "workspace.raw.products")
    assert result[0] == watermark_data[0][1]

    # Test non-existent table
    result = get_watermark(spark_session, watermark_table, "non.existent.table")
    assert result is None

    # Clean up
    spark_session.sql(f"DROP TABLE IF EXISTS {watermark_table}")

def test_load_raw_products(spark_session, sample_raw_products):
    # Setup test raw table
    raw_table = "test_raw_products"
    sample_raw_products.write.format("delta").saveAsTable(raw_table)

    # Test loading with watermark
    watermark_value = (datetime.now() - timedelta(days=1.5),)
    result = load_raw_products(spark_session, raw_table, watermark_value)
    assert result.count() == 1  # Only the newest record should be returned

    # Test loading without watermark
    result = load_raw_products(spark_session, raw_table, None)
    assert result.count() == 3

    # Clean up
    spark_session.sql(f"DROP TABLE IF EXISTS {raw_table}")

def test_transform_products_success(spark_session, sample_raw_products):
    # Test successful transformation
    result = transform_products(sample_raw_products)

    # Verify column transformations
    assert result.count() == 3
    assert set(result.columns) == {"productid", "category", "subcategory", "inserttimestamp", "updatetimestamp"}

    # Verify data cleaning
    prod1 = result.filter("productid = 'PROD1'").first()
    assert prod1["category"] == "Electronics"
    assert prod1["subcategory"] == "Laptops"

def test_transform_products_empty_input(spark_session, empty_raw_products):
    # Test empty input
    result = transform_products(empty_raw_products)
    assert result.count() == 0

def test_transform_products_null_values(spark_session):
    # Test null handling
    schema = StructType([
        StructField("productid", StringType(), True),
        StructField("category", StringType(), True),
        StructField("subcategory", StringType(), True),
        StructField("inserttimestamp", TimestampType(), True)
    ])

    data = [
        ("prod1", None, "Laptops", datetime.now()),
        (None, "Electronics", "Laptops", datetime.now()),
        ("prod3", "Clothing", None, datetime.now())
    ]

    df = spark_session.createDataFrame(data, schema)
    result = transform_products(df)
    assert result.count() == 0  # All rows should be filtered out

def test_transform_products_empty_strings(spark_session):
    # Test empty string handling
    schema = StructType([
        StructField("productid", StringType(), True),
        StructField("category", StringType(), True),
        StructField("subcategory", StringType(), True),
        StructField("inserttimestamp", TimestampType(), True)
    ])

    data = [
        ("prod1", "", "Laptops", datetime.now()),
        ("prod2", "Electronics", "", datetime.now()),
        ("prod3", "Clothing", "Shirts", datetime.now())
    ]

    df = spark_session.createDataFrame(data, schema)
    result = transform_products(df)

    # Verify empty strings were replaced with "Unknown"
    prod1 = result.filter("productid = 'PROD1'").first()
    assert prod1["category"] == "Unknown"

    prod2 = result.filter("productid = 'PROD2'").first()
    assert prod2["subcategory"] == "Unknown"

def test_transform_products_deduplication(spark_session):
    # Test deduplication
    schema = StructType([
        StructField("productid", StringType(), True),
        StructField("category", StringType(), True),
        StructField("subcategory", StringType(), True),
        StructField("inserttimestamp", TimestampType(), True)
    ])

    data = [
        ("prod1", "Electronics", "Laptops", datetime.now() - timedelta(hours=2)),
        ("prod1", "Electronics", "Ultrabooks", datetime.now() - timedelta(hours=1)),
        ("prod2", "Furniture", "Chairs", datetime.now())
    ]

    df = spark_session.createDataFrame(data, schema)
    result = transform_products(df)

    # Should keep only the latest record for each product
    assert result.count() == 2
    prod1 = result.filter("productid = 'PROD1'").first()
    assert prod1["subcategory"] == "Ultrabooks"

def test_merge_products_new_table(spark_session, sample_raw_products):
    # Test merge into new table
    test_table = "test_enriched_products_new"
    enriched_df = transform_products(sample_raw_products)

    merge_products(spark_session, enriched_df, test_table)

    # Verify table was created
    result = spark_session.table(test_table)
    assert result.count() == 3

    # Clean up
    spark_session.sql(f"DROP TABLE IF EXISTS {test_table}")

def test_merge_products_existing_table(spark_session, sample_raw_products):
    # Test merge into existing table
    test_table = "test_enriched_products_existing"

    # First create the table
    enriched_df = transform_products(sample_raw_products)
    merge_products(spark_session, enriched_df, test_table)

    # Now update one record and add a new one
    updated_data = [
        ("prod1", "Electronics", "Gaming Laptops", datetime.now()),
        ("prod4", "Appliances", "Refrigerators", datetime.now())
    ]
    updated_df = spark_session.createDataFrame(updated_data, sample_raw_products.schema)
    updated_enriched = transform_products(updated_df)

    merge_products(spark_session, updated_enriched, test_table)

    # Verify updates
    result = spark_session.table(test_table)
    assert result.count() == 4
    prod1 = result.filter("productid = 'PROD1'").first()
    assert prod1["subcategory"] == "Gaming Laptops"

    # Clean up
    spark_session.sql(f"DROP TABLE IF EXISTS {test_table}")

def test_update_watermark(spark_session):
    # Setup test watermark table
    watermark_table = "test_watermark_update"
    watermark_schema = StructType([
        StructField("table_name", StringType(), True),
        StructField("watermark_value", TimestampType(), True),
        StructField("updated_at", TimestampType(), True)
    ])

    watermark_data = [("workspace.raw.products", datetime.now() - timedelta(days=2), datetime.now())]
    watermark_df = spark_session.createDataFrame(watermark_data, watermark_schema)
    watermark_df.write.format("delta").saveAsTable(watermark_table)

    # Test updating watermark
    new_ts = datetime.now() - timedelta(days=1)
    update_watermark(spark_session, watermark_table, "workspace.raw.products", new_ts)

    # Verify update
    result = spark_session.table(watermark_table).filter("table_name = 'workspace.raw.products'").first()
    assert result["watermark_value"] == new_ts

    # Clean up
    spark_session.sql(f"DROP TABLE IF EXISTS {watermark_table}")

def test_quarantine_failed_records(spark_session, sample_raw_products):
    # Setup test data with some records that will fail transformation
    schema = StructType([
        StructField("productid", StringType(), True),
        StructField("category", StringType(), True),
        StructField("subcategory", StringType(), True),
        StructField("inserttimestamp", TimestampType(), True)
    ])

    data = [
        ("prod1", "Electronics", "Laptops", datetime.now()),  # Will pass
        ("prod2", None, "Chairs", datetime.now()),  # Will fail (null category)
        ("prod3", "Clothing", None, datetime.now())  # Will fail (null subcategory)
    ]

    raw_df = spark_session.createDataFrame(data, schema)
    enriched_df = transform_products(raw_df)

    # Test quarantine
    quarantine_failed_records(spark_session, raw_df, enriched_df)

    # Verify quarantined records
    quarantine_table = "workspace.quarantine.products"
    if spark_session.catalog.tableExists(quarantine_table):
        result = spark_session.table(quarantine_table)
        assert result.count() == 2  # Two failed records

        # Clean up
        spark_session.sql(f"DROP TABLE IF EXISTS {quarantine_table}")

def test_run_products_enrichment(spark_session):
    # Setup test tables
    raw_table = "test_raw_products"
    enriched_table = "test_enriched_products"
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
        StructField("productid", StringType(), True),
        StructField("category", StringType(), True),
        StructField("subcategory", StringType(), True),
        StructField("inserttimestamp", TimestampType(), True)
    ])

    raw_data = [
        ("prod1", "Electronics", "Laptops", datetime.now() - timedelta(days=2)),  # Old record
        ("prod2", "Furniture", "Chairs", datetime.now() - timedelta(days=1)),  # Old record
        ("prod3", "Clothing", "Shirts", datetime.now())  # New record
    ]
    raw_df = spark_session.createDataFrame(raw_data, raw_schema)
    raw_df.write.format("delta").saveAsTable(raw_table)

    # Run the pipeline
    run_products_enrichment(spark_session, raw_table, enriched_table, watermark_table)

    # Verify results
    enriched_result = spark_session.table(enriched_table)
    assert enriched_result.count() == 1  # Only the new record should be processed

    watermark_result = spark_session.table(watermark_table)
    assert watermark_result.filter(f"table_name = '{raw_table}'").count() == 1

    # Clean up
    spark_session.sql(f"DROP TABLE IF EXISTS {raw_table}")
    spark_session.sql(f"DROP TABLE IF EXISTS {enriched_table}")
    spark_session.sql(f"DROP TABLE IF EXISTS {watermark_table}")
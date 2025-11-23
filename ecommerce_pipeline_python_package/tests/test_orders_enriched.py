# tests/test_orders_enriched.py
import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, DateType
)
from pyspark.sql import Row
from pei.silver.orders_enriched import transform_orders, write_orders_enriched
import tempfile
from datetime import datetime, timedelta

@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession.builder \
        .appName("test_orders_enriched") \
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
def sample_raw_orders(spark_session):
    schema = StructType([
        StructField("orderid", StringType(), True),
        StructField("orderdate", StringType(), True),
        StructField("shipdate", StringType(), True),
        StructField("shipmode", StringType(), True),
        StructField("customerid", StringType(), True),
        StructField("productid", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("discount", DoubleType(), True),
        StructField("profit", DoubleType(), True),
        StructField("inserttimestamp", TimestampType(), True)
    ])

    data = [
        # Valid record
        ("ORD-123", "1/5/2023", "5/5/2023", "standard", "CUST-001", "PROD-001", 10, 99.99, 0.1, 89.99, datetime.now()),
        # Invalid ship date (before order date)
        ("ORD-124", "10/5/2023", "5/5/2023", "express", "CUST-002", "PROD-002", 5, 49.99, 0.0, 24.99, datetime.now()),
        # Invalid quantity (zero)
        ("ORD-125", "15/5/2023", "20/5/2023", "standard", "CUST-003", "PROD-003", 0, 19.99, 0.1, 1.8, datetime.now()),
        # Invalid discount (>1)
        ("ORD-126", "20/5/2023", "25/5/2023", "priority", "CUST-004", "PROD-004", 3, 29.99, 1.1, -3.0, datetime.now()),
        # Duplicate record (should be deduplicated)
        ("ORD-123", "1/5/2023", "5/5/2023", "standard", "CUST-001", "PROD-001", 10, 99.99, 0.1, 89.99, datetime.now() + timedelta(hours=1)),
        # Null order date
        ("ORD-127", None, "25/5/2023", "standard", "CUST-005", "PROD-005", 2, 19.99, 0.0, 3.99, datetime.now()),
        # Null customer ID
        ("ORD-128", "25/5/2023", "30/5/2023", "express", None, "PROD-006", 1, 9.99, 0.0, 0.99, datetime.now())
    ]

    return spark_session.createDataFrame(data, schema)

@pytest.fixture
def empty_raw_orders(spark_session):
    schema = StructType([
        StructField("orderid", StringType(), True),
        StructField("orderdate", StringType(), True),
        StructField("shipdate", StringType(), True),
        StructField("shipmode", StringType(), True),
        StructField("customerid", StringType(), True),
        StructField("productid", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("discount", DoubleType(), True),
        StructField("profit", DoubleType(), True),
        StructField("inserttimestamp", TimestampType(), True)
    ])
    return spark_session.createDataFrame([], schema)

def test_transform_orders_success(spark_session, sample_raw_orders):
    # Test successful transformation
    result = transform_orders(sample_raw_orders)

    # Verify column transformations
    assert result.count() == 1  # Only 1 valid record should remain
    assert set(result.columns) == {
        "orderid", "orderdate", "shipdate", "shipmode", "customerid",
        "productid", "quantity", "price", "discount", "profit",
        "inserttimestamp", "updatetimestamp", "orderyear"
    }

    # Verify data cleaning
    row = result.first()
    assert row["orderid"] == "ORD123"
    assert row["customerid"] == "CUST001"
    assert row["productid"] == "PROD001"
    assert row["shipmode"] == "Standard"
    assert row["orderyear"] == 2023
    assert isinstance(row["orderdate"], datetime)
    assert isinstance(row["shipdate"], datetime)
    assert row["quantity"] == 10
    assert row["price"] == 99.99
    assert row["discount"] == 0.1
    assert row["profit"] == 89.99

def test_transform_orders_empty_input(spark_session, empty_raw_orders):
    # Test empty input
    result = transform_orders(empty_raw_orders)
    assert result.count() == 0

def test_transform_orders_missing_columns(spark_session):
    # Test missing required columns
    schema = StructType([
        StructField("orderid", StringType(), True),
        StructField("orderdate", StringType(), True)
    ])
    df = spark_session.createDataFrame([], schema)

    with pytest.raises(ValueError, match="Missing required columns"):
        transform_orders(df)

def test_transform_orders_date_conversion(spark_session):
    # Test date conversion
    schema = StructType([
        StructField("orderid", StringType(), True),
        StructField("orderdate", StringType(), True),
        StructField("shipdate", StringType(), True),
        StructField("shipmode", StringType(), True),
        StructField("customerid", StringType(), True),
        StructField("productid", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("discount", DoubleType(), True),
        StructField("profit", DoubleType(), True),
        StructField("inserttimestamp", TimestampType(), True)
    ])

    data = [
        ("ORD-123", "1/5/2023", "5/5/2023", "standard", "CUST-001", "PROD-001", 10, 99.99, 0.1, 89.99, datetime.now())
    ]
    df = spark_session.createDataFrame(data, schema)
    result = transform_orders(df)

    row = result.first()
    assert isinstance(row["orderdate"], datetime)
    assert isinstance(row["shipdate"], datetime)
    assert row["orderyear"] == 2023

def test_transform_orders_id_cleaning(spark_session):
    # Test ID cleaning
    schema = StructType([
        StructField("orderid", StringType(), True),
        StructField("orderdate", StringType(), True),
        StructField("shipdate", StringType(), True),
        StructField("shipmode", StringType(), True),
        StructField("customerid", StringType(), True),
        StructField("productid", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("discount", DoubleType(), True),
        StructField("profit", DoubleType(), True),
        StructField("inserttimestamp", TimestampType(), True)
    ])

    data = [
        ("  ord-123  ", "1/5/2023", "5/5/2023", "standard", "  cust-001  ", "  prod-001  ", 10, 99.99, 0.1, 89.99, datetime.now())
    ]
    df = spark_session.createDataFrame(data, schema)
    result = transform_orders(df)

    row = result.first()
    assert row["orderid"] == "ORD123"
    assert row["customerid"] == "CUST001"
    assert row["productid"] == "PROD001"

def test_transform_orders_ship_date_validation(spark_session):
    # Test ship date validation
    schema = StructType([
        StructField("orderid", StringType(), True),
        StructField("orderdate", StringType(), True),
        StructField("shipdate", StringType(), True),
        StructField("shipmode", StringType(), True),
        StructField("customerid", StringType(), True),
        StructField("productid", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("discount", DoubleType(), True),
        StructField("profit", DoubleType(), True),
        StructField("inserttimestamp", TimestampType(), True)
    ])

    data = [
        # Valid (ship date after order date)
        ("ORD-123", "1/5/2023", "5/5/2023", "standard", "CUST-001", "PROD-001", 10, 99.99, 0.1, 89.99, datetime.now()),
        # Invalid (ship date before order date)
        ("ORD-124", "10/5/2023", "5/5/2023", "express", "CUST-002", "PROD-002", 5, 49.99, 0.0, 24.99, datetime.now())
    ]
    df = spark_session.createDataFrame(data, schema)
    result = transform_orders(df)

    assert result.count() == 1  # Only the valid record should remain

def test_transform_orders_numeric_validation(spark_session):
    # Test numeric field validation
    schema = StructType([
        StructField("orderid", StringType(), True),
        StructField("orderdate", StringType(), True),
        StructField("shipdate", StringType(), True),
        StructField("shipmode", StringType(), True),
        StructField("customerid", StringType(), True),
        StructField("productid", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("discount", DoubleType(), True),
        StructField("profit", DoubleType(), True),
        StructField("inserttimestamp", TimestampType(), True)
    ])

    data = [
        # Valid record
        ("ORD-123", "1/5/2023", "5/5/2023", "standard", "CUST-001", "PROD-001", 10, 99.99, 0.1, 89.99, datetime.now()),
        # Invalid quantity (zero)
        ("ORD-124", "1/5/2023", "5/5/2023", "standard", "CUST-002", "PROD-002", 0, 49.99, 0.0, 24.99, datetime.now()),
        # Invalid price (negative)
        ("ORD-125", "1/5/2023", "5/5/2023", "standard", "CUST-003", "PROD-003", 5, -19.99, 0.1, -1.8, datetime.now()),
        # Invalid discount (>1)
        ("ORD-126", "1/5/2023", "5/5/2023", "standard", "CUST-004", "PROD-004", 3, 29.99, 1.1, -3.0, datetime.now()),
        # Invalid discount (<0)
        ("ORD-127", "1/5/2023", "5/5/2023", "standard", "CUST-005", "PROD-005", 2, 19.99, -0.1, 3.99, datetime.now())
    ]
    df = spark_session.createDataFrame(data, schema)
    result = transform_orders(df)

    assert result.count() == 1  # Only the valid record should remain

def test_transform_orders_deduplication(spark_session):
    # Test deduplication
    schema = StructType([
        StructField("orderid", StringType(), True),
        StructField("orderdate", StringType(), True),
        StructField("shipdate", StringType(), True),
        StructField("shipmode", StringType(), True),
        StructField("customerid", StringType(), True),
        StructField("productid", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("discount", DoubleType(), True),
        StructField("profit", DoubleType(), True),
        StructField("inserttimestamp", TimestampType(), True)
    ])

    data = [
        # Original record
        ("ORD-123", "1/5/2023", "5/5/2023", "standard", "CUST-001", "PROD-001", 10, 99.99, 0.1, 89.99, datetime.now() - timedelta(hours=1)),
        # Duplicate with newer timestamp
        ("ORD-123", "1/5/2023", "5/5/2023", "express", "CUST-001", "PROD-001", 5, 99.99, 0.0, 49.99, datetime.now())
    ]
    df = spark_session.createDataFrame(data, schema)
    result = transform_orders(df)

    assert result.count() == 1  # Only one record should remain
    row = result.first()
    assert row["shipmode"] == "Express"  # Should keep the newer record
    assert row["quantity"] == 5

def test_write_orders_enriched(spark_session, sample_raw_orders):
    # Test writing enriched orders
    test_table = "test_enriched_orders"
    enriched_df = transform_orders(sample_raw_orders)

    # Test write
    write_orders_enriched(enriched_df, test_table)

    # Verify table exists and has data
    result = spark_session.table(test_table)
    assert result.count() == 1

    # Clean up
    spark_session.sql(f"DROP TABLE IF EXISTS {test_table}")

def test_write_orders_enriched_overwrite(spark_session, sample_raw_orders):
    # Test overwrite mode
    test_table = "test_enriched_orders_overwrite"
    enriched_df = transform_orders(sample_raw_orders)

    # First write
    write_orders_enriched(enriched_df, test_table)

    # Second write with different data
    new_data = [
        ("ORD-456", "1/6/2023", "6/6/2023", "standard", "CUST-002", "PROD-002", 8, 79.99, 0.1, 71.99, datetime.now())
    ]
    new_schema = sample_raw_orders.schema
    new_df = spark_session.createDataFrame(new_data, new_schema)
    new_enriched = transform_orders(new_df)

    # Overwrite
    write_orders_enriched(new_enriched, test_table, mode="overwrite")

    # Verify overwrite
    result = spark_session.table(test_table)
    assert result.count() == 1
    row = result.first()
    assert row["orderid"] == "ORD456"

    # Clean up
    spark_session.sql(f"DROP TABLE IF EXISTS {test_table}")
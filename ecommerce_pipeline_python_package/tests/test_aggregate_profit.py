# tests/test_aggregate_profit.py
import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType
)
from pyspark.sql import Row
from delta.tables import DeltaTable
from pei.gold.aggregate_profit import (
    safe_max_ts, rows_since, get_watermarks, load_enriched_tables,
    full_recompute, update_customer_names, incremental_aggregate,
    update_watermarks, run_aggregate_profit
)
import tempfile
from datetime import datetime, timedelta

@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession.builder \
        .appName("test_aggregate_profit") \
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
def sample_data(spark_session):
    # Orders schema
    orders_schema = StructType([
        StructField("orderid", StringType(), True),
        StructField("orderyear", IntegerType(), True),
        StructField("customerid", StringType(), True),
        StructField("productid", StringType(), True),
        StructField("profit", DoubleType(), True),
        StructField("updatetimestamp", TimestampType(), True)
    ])

    # Products schema
    products_schema = StructType([
        StructField("productid", StringType(), True),
        StructField("category", StringType(), True),
        StructField("subcategory", StringType(), True),
        StructField("updatetimestamp", TimestampType(), True)
    ])

    # Customers schema
    customers_schema = StructType([
        StructField("customerid", StringType(), True),
        StructField("customername", StringType(), True),
        StructField("country", StringType(), True),
        StructField("updatetimestamp", TimestampType(), True)
    ])

    # Sample data
    orders_data = [
        ("order1", 2023, "cust1", "prod1", 100.0, datetime.now() - timedelta(days=2)),
        ("order2", 2023, "cust2", "prod2", 200.0, datetime.now() - timedelta(days=1)),
        ("order3", 2023, "cust1", "prod3", 150.0, datetime.now())
    ]

    products_data = [
        ("prod1", "Electronics", "Laptops", datetime.now() - timedelta(days=3)),
        ("prod2", "Furniture", "Chairs", datetime.now() - timedelta(days=2)),
        ("prod3", "Clothing", "Shirts", datetime.now() - timedelta(days=1))
    ]

    customers_data = [
        ("cust1", "John Doe", "USA", datetime.now() - timedelta(days=4)),
        ("cust2", "Jane Smith", "UK", datetime.now() - timedelta(days=3))
    ]

    return {
        "orders": spark_session.createDataFrame(orders_data, orders_schema),
        "products": spark_session.createDataFrame(products_data, products_schema),
        "customers": spark_session.createDataFrame(customers_data, customers_schema)
    }

@pytest.fixture
def setup_tables(spark_session, sample_data):
    # Create test tables
    orders_table = "test_enriched_orders"
    products_table = "test_enriched_products"
    customers_table = "test_enriched_customers"
    aggregate_table = "test_aggregate_profits"
    watermark_table = "test_watermark"

    # Write sample data to tables
    sample_data["orders"].write.format("delta").saveAsTable(orders_table)
    sample_data["products"].write.format("delta").saveAsTable(products_table)
    sample_data["customers"].write.format("delta").saveAsTable(customers_table)

    # Create watermark table
    watermark_schema = StructType([
        StructField("table_name", StringType(), True),
        StructField("column_name", StringType(), True),
        StructField("watermark_value", TimestampType(), True),
        StructField("updated_at", TimestampType(), True)
    ])

    watermark_data = [
        (orders_table, "updatetimestamp", datetime.now() - timedelta(days=5), datetime.now()),
        (products_table, "updatetimestamp", datetime.now() - timedelta(days=4), datetime.now()),
        (customers_table, "updatetimestamp", datetime.now() - timedelta(days=3), datetime.now())
    ]
    watermark_df = spark_session.createDataFrame(watermark_data, watermark_schema)
    watermark_df.write.format("delta").saveAsTable(watermark_table)

    yield {
        "orders_table": orders_table,
        "products_table": products_table,
        "customers_table": customers_table,
        "aggregate_table": aggregate_table,
        "watermark_table": watermark_table
    }

    # Clean up
    spark_session.sql(f"DROP TABLE IF EXISTS {orders_table}")
    spark_session.sql(f"DROP TABLE IF EXISTS {products_table}")
    spark_session.sql(f"DROP TABLE IF EXISTS {customers_table}")
    spark_session.sql(f"DROP TABLE IF EXISTS {aggregate_table}")
    spark_session.sql(f"DROP TABLE IF EXISTS {watermark_table}")

def test_safe_max_ts(spark_session):
    # Test with data
    schema = StructType([StructField("ts", TimestampType(), True)])
    data = [(datetime.now(),), (datetime.now() - timedelta(days=1),)]
    df = spark_session.createDataFrame(data, schema)
    result = safe_max_ts(df, "ts")
    assert result is not None

    # Test with empty DataFrame
    empty_df = spark_session.createDataFrame([], schema)
    result = safe_max_ts(empty_df, "ts")
    assert result is None

def test_rows_since(spark_session):
    # Test with watermark
    schema = StructType([StructField("ts", TimestampType(), True)])
    data = [
        (datetime.now(),),
        (datetime.now() - timedelta(days=1),),
        (datetime.now() - timedelta(days=2),)
    ]
    df = spark_session.createDataFrame(data, schema)
    watermark = datetime.now() - timedelta(days=1.5)
    result = rows_since(df, "ts", watermark)
    assert result.count() == 1

    # Test without watermark
    result = rows_since(df, "ts", None)
    assert result.count() == 3

def test_get_watermarks(spark_session):
    # Setup test watermark table
    watermark_table = "test_watermarks"
    watermark_schema = StructType([
        StructField("table_name", StringType(), True),
        StructField("watermark_value", TimestampType(), True)
    ])

    watermark_data = [
        ("table1", datetime.now() - timedelta(days=1)),
        ("table2", datetime.now() - timedelta(days=2))
    ]
    watermark_df = spark_session.createDataFrame(watermark_data, watermark_schema)
    watermark_df.write.format("delta").saveAsTable(watermark_table)

    # Test getting watermarks
    result = get_watermarks(spark_session, watermark_table, ["table1", "table2"])
    assert len(result) == 2
    assert result["table1"] == watermark_data[0][1]

    # Clean up
    spark_session.sql(f"DROP TABLE IF EXISTS {watermark_table}")

def test_load_enriched_tables(spark_session, sample_data):
    # Setup test tables
    orders_table = "test_orders"
    products_table = "test_products"
    customers_table = "test_customers"

    sample_data["orders"].write.format("delta").saveAsTable(orders_table)
    sample_data["products"].write.format("delta").saveAsTable(products_table)
    sample_data["customers"].write.format("delta").saveAsTable(customers_table)

    # Define tables and columns
    tables = {
        orders_table: ["orderid", "orderyear", "customerid", "productid", "profit", "updatetimestamp"],
        products_table: ["productid", "category", "subcategory", "updatetimestamp"],
        customers_table: ["customerid", "customername", "country", "updatetimestamp"]
    }

    # Test loading tables
    result = load_enriched_tables(spark_session, tables)
    assert len(result) == 3
    assert result[orders_table].count() == 3
    assert result[products_table].count() == 3
    assert result[customers_table].count() == 2

    # Clean up
    spark_session.sql(f"DROP TABLE IF EXISTS {orders_table}")
    spark_session.sql(f"DROP TABLE IF EXISTS {products_table}")
    spark_session.sql(f"DROP TABLE IF EXISTS {customers_table}")

def test_full_recompute(spark_session, sample_data):
    # Setup test aggregate table
    aggregate_table = "test_aggregate"

    # Test full recompute
    full_recompute(
        sample_data["orders"],
        sample_data["products"],
        sample_data["customers"],
        aggregate_table
    )

    # Verify results
    result = spark_session.table(aggregate_table)
    assert result.count() == 3  # 3 orders with 2 unique customers

    # Verify aggregation
    cust1_profit = result.filter("customerid = 'cust1'").agg(F.sum("totalprofit")).first()[0]
    assert cust1_profit == 250.0  # 100 + 150

    # Clean up
    spark_session.sql(f"DROP TABLE IF EXISTS {aggregate_table}")

def test_update_customer_names(spark_session, sample_data):
    # Setup test aggregate table
    aggregate_table = "test_aggregate"
    full_recompute(
        sample_data["orders"],
        sample_data["products"],
        sample_data["customers"],
        aggregate_table
    )

    # Create changed customers data
    customers_schema = StructType([
        StructField("customerid", StringType(), True),
        StructField("customername", StringType(), True),
        StructField("updatetimestamp", TimestampType(), True)
    ])

    changed_customers = [
        ("cust1", "John Doe Updated", datetime.now())
    ]
    changed_df = spark_session.createDataFrame(changed_customers, customers_schema)

    # Test customer name update
    update_customer_names(spark_session, changed_df, aggregate_table)

    # Verify update
    result = spark_session.table(aggregate_table)
    cust1 = result.filter("customerid = 'cust1'").first()
    assert cust1["customername"] == "John Doe Updated"

    # Clean up
    spark_session.sql(f"DROP TABLE IF EXISTS {aggregate_table}")

def test_incremental_aggregate(spark_session, sample_data):
    # Setup test aggregate table
    aggregate_table = "test_aggregate"
    full_recompute(
        sample_data["orders"],
        sample_data["products"],
        sample_data["customers"],
        aggregate_table
    )

    # Create changed orders data
    orders_schema = StructType([
        StructField("orderid", StringType(), True),
        StructField("orderyear", IntegerType(), True),
        StructField("customerid", StringType(), True),
        StructField("productid", StringType(), True),
        StructField("profit", DoubleType(), True),
        StructField("updatetimestamp", TimestampType(), True)
    ])

    changed_orders = [
        ("order4", 2023, "cust1", "prod1", 50.0, datetime.now())
    ]
    changed_df = spark_session.createDataFrame(changed_orders, orders_schema)

    # Test incremental aggregation
    incremental_aggregate(
        spark_session,
        changed_df,
        sample_data["products"],
        sample_data["customers"],
        aggregate_table
    )

    # Verify incremental update
    result = spark_session.table(aggregate_table)
    cust1 = result.filter("customerid = 'cust1'").first()
    assert cust1["totalprofit"] == 300.0  # 250 + 50

    # Clean up
    spark_session.sql(f"DROP TABLE IF EXISTS {aggregate_table}")

def test_update_watermarks(spark_session):
    # Setup test watermark table
    watermark_table = "test_watermark_update"
    watermark_schema = StructType([
        StructField("table_name", StringType(), True),
        StructField("column_name", StringType(), True),
        StructField("watermark_value", TimestampType(), True),
        StructField("updated_at", TimestampType(), True)
    ])

    watermark_data = [
        ("table1", "ts", datetime.now() - timedelta(days=2), datetime.now())
    ]
    watermark_df = spark_session.createDataFrame(watermark_data, watermark_schema)
    watermark_df.write.format("delta").saveAsTable(watermark_table)

    # Test updating watermarks
    table_updates = {
        "table1": ("ts", datetime.now() - timedelta(days=1)),
        "table2": ("ts", datetime.now())
    }
    update_watermarks(spark_session, watermark_table, table_updates)

    # Verify updates
    result = spark_session.table(watermark_table)
    assert result.count() == 2
    table1 = result.filter("table_name = 'table1'").first()
    assert table1["watermark_value"] == datetime.now() - timedelta(days=1)

    # Clean up
    spark_session.sql(f"DROP TABLE IF EXISTS {watermark_table}")

def test_run_aggregate_profit_full_recompute(spark_session, setup_tables):
    # Test full recompute scenario (products changed)
    tables = setup_tables

    # Update a product to trigger full recompute
    products_df = spark_session.table(tables["products_table"])
    updated_products = products_df.withColumn(
        "subcategory",
        F.when(F.col("productid") == "prod1", "Gaming Laptops").otherwise(F.col("subcategory"))
    ).withColumn("updatetimestamp", F.current_timestamp())

    updated_products.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(tables["products_table"])

    # Run the pipeline
    run_aggregate_profit(
        spark_session,
        tables["orders_table"],
        tables["products_table"],
        tables["customers_table"],
        tables["aggregate_table"],
        tables["watermark_table"]
    )

    # Verify results
    result = spark_session.table(tables["aggregate_table"])
    assert result.count() == 3

    # Verify watermarks were updated
    watermarks = spark_session.table(tables["watermark_table"])
    assert watermarks.count() == 3

def test_run_aggregate_profit_customer_update(spark_session, setup_tables):
    # Test customer update scenario
    tables = setup_tables

    # Update a customer to trigger name update
    customers_df = spark_session.table(tables["customers_table"])
    updated_customers = customers_df.withColumn(
        "customername",
        F.when(F.col("customerid") == "cust1", "John Doe Updated").otherwise(F.col("customername"))
    ).withColumn("updatetimestamp", F.current_timestamp())

    updated_customers.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(tables["customers_table"])

    # Run the pipeline
    run_aggregate_profit(
        spark_session,
        tables["orders_table"],
        tables["products_table"],
        tables["customers_table"],
        tables["aggregate_table"],
        tables["watermark_table"]
    )

    # Verify customer name was updated
    result = spark_session.table(tables["aggregate_table"])
    cust1 = result.filter("customerid = 'cust1'").first()
    assert cust1["customername"] == "John Doe Updated"

def test_run_aggregate_profit_incremental(spark_session, setup_tables):
    # Test incremental scenario (orders changed)
    tables = setup_tables

    # Add a new order to trigger incremental update
    orders_schema = StructType([
        StructField("orderid", StringType(), True),
        StructField("orderyear", IntegerType(), True),
        StructField("customerid", StringType(), True),
        StructField("productid", StringType(), True),
        StructField("profit", DoubleType(), True),
        StructField("updatetimestamp", TimestampType(), True)
    ])

    new_order = [("order4", 2023, "cust1", "prod1", 50.0, datetime.now())]
    new_df = spark_session.createDataFrame(new_order, orders_schema)
    new_df.write.format("delta") \
        .mode("append") \
        .saveAsTable(tables["orders_table"])

    # Run the pipeline
    run_aggregate_profit(
        spark_session,
        tables["orders_table"],
        tables["products_table"],
        tables["customers_table"],
        tables["aggregate_table"],
        tables["watermark_table"]
    )

    # Verify incremental update
    result = spark_session.table(tables["aggregate_table"])
    cust1 = result.filter("customerid = 'cust1'").first()
    assert cust1["totalprofit"] == 300.0  # 250 + 50
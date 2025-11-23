import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pei.raw.orders_raw import load_raw_products
from pathlib import Path

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("orders_raw_tests")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )

def test_load_orders_basic(spark, tmp_path):
    """
    Test that loader can read multiline JSON 
    and returns expected columns.
    """
    sample_json = """
    [
      {
        "Row ID": 1,
        "Order ID": "CA-2016-122581",
        "Order Date": "21/8/2016",
        "Ship Date": "25/8/2016",
        "Ship Mode": "Standard Class",
        "Customer ID": "JK-15370",
        "Product ID": "FUR-CH-10002961",
        "Quantity": 7,
        "Price": 573.17,
        "Discount": 0.3,
        "Profit": 63.69
      }
    ]
    """
    test_file = tmp_path / "orders.json"
    test_file.write_text(sample_json)

    df = load_raw_products(spark, str(test_file))

    expected_cols = {
        "rowid", "orderid", "orderdate", "shipdate", "shipmode",
        "customerid", "productid", "quantity", "price", "discount",
        "profit", "inserttimestamp"
    }

    assert expected_cols.issubset(set(df.columns))
    assert df.count() == 1, f"Expected 1 row but got {df.count()}"

def test_schema_evolution_new_column(spark, tmp_path):
    """
    Test that loader handles schema evolution (extra new column)
    because json() + mergeSchema at write handles it.
    """
    sample_json = """
    [
      {
        "Row ID": 2,
        "Order ID": "CA-2017-117485",
        "Order Date": "23/9/2017",
        "ExtraColumn": "SOME_NEW_VALUE"
      }
    ]
    """
    test_file = tmp_path / "orders_newcol.json"
    test_file.write_text(sample_json)

    df = load_raw_products(spark, str(test_file))

    assert "extracolumn" in df.columns, f"Schema evolution failed. Expected new column 'extracolumn' in DataFrame. Columns present: {df.columns}"

    value = df.select("extracolumn").first()[0]
    assert value == "SOME_NEW_VALUE", f"Expected ExtraColumn value 'SOME_NEW_VALUE' but got '{value}'"


def test_corrupt_record_handling(spark, tmp_path):
    """
    Broken JSON must land in columnNameOfCorruptRecord ('_corrupt_record')
    """
    invalid_json = """{ "Row ID": 10, "Order ID": "BAD_JSON"  """  # missing end braces

    test_file = tmp_path / "orders_bad.json"
    test_file.write_text(invalid_json)

    df = load_raw_products(spark, str(test_file))

    assert "_corrupt_record" in df.columns, f"Expected '_corrupt_record' column but found: {df.columns}"
    
    corrupted_count = df.filter(F.col("_corrupt_record").isNotNull()).count()
    assert corrupted_count == 1, f"Corrupt JSON not captured. Expected 1 corrupt row but found {corrupted_count}"

def test_null_values_handled(spark, tmp_path):
    """
    Test null and missing fields load properly.
    """
    null_json = """
    [
      {
        "Row ID": null,
        "Order ID": null
      }
    ]
    """
    test_file = tmp_path / "orders_null.json"
    test_file.write_text(null_json)

    df = load_raw_products(spark, str(test_file))

    assert df.count() == 1, f"Expected 1 row but got {df.count()}"
    
    null_count = df.filter(F.col("orderid").isNull()).count()
    assert null_count == 1, f"Expected column orderid to be NULL but found {null_count} NULL rows"

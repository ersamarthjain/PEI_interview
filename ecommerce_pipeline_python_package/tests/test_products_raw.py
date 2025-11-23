import pytest
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pei.raw.products_raw import load_raw_products


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("products-raw-test") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()

def test_schema_evolution_new_column(spark, tmp_path):

    tmp_csv = tmp_path / "Products.csv"

    pdf = pd.DataFrame({
        "Product ID": ["P1"],
        "Category": ["Office Supplies"],
        "Sub-Category": ["Paper"],
        "Product Name": ["Copy Paper"],
        "State": ["California"],
        "Price per product": [8.56],
        "New Column": ["ExtraValue"]   # <-- New column added
    })
    pdf.to_csv(tmp_csv, index=False)

    df = load_raw_products(spark, str(tmp_csv))

    expected_cols = {
        "productid",
        "category",
        "subcategory",
        "productname",
        "state",
        "priceperproduct",
        "newcolumn",          # schema evolution
        "inserttimestamp"
    }

    missing = expected_cols - set(df.columns)
    assert not missing, f"Missing expected columns after schema evolution: {missing}"

    assert df.first().newcolumn == "ExtraValue", "Schema evolution failed to retain new column value"


def test_missing_columns_are_absent(spark, tmp_path):

    tmp_csv = tmp_path / "Products_missing.csv"

    pdf = pd.DataFrame({
        "Product ID": ["P2"],
        "Category": ["Furniture"],
        "Sub-Category": ["Chairs"]
    })
    pdf.to_csv(tmp_csv, index=False)

    df = load_raw_products(spark, str(tmp_csv))

    assert "productid" in df.columns
    assert "category" in df.columns
    assert "subcategory" in df.columns

    assert "productname" not in df.columns, "Unexpected column 'productname' appeared but was not in source file"


def test_null_and_empty_values(spark, tmp_path):

    tmp_csv = tmp_path / "Products_nulls.csv"

    pdf = pd.DataFrame({
        "Product ID": [None],
        "Category": [""],
        "Sub-Category": ["Paper"],
        "Product Name": [None],
        "State": ["Texas"],
        "Price per product": [None]
    })
    pdf.to_csv(tmp_csv, index=False)

    df = load_raw_products(spark, str(tmp_csv))

    row = df.first()

    assert row.productid is None, "Expected NULL productid but got non-null value"
    assert row.category == "", "Expected empty string in category"
    assert row.priceperproduct is None, "Expected NULL priceperproduct but got value"


def test_duplicate_rows_preserved(spark, tmp_path):

    tmp_csv = tmp_path / "Products_dups.csv"

    pdf = pd.DataFrame({
        "Product ID": ["P1", "P1"],
        "Category": ["Office", "Office"],
        "Sub-Category": ["Paper", "Paper"],
        "Product Name": ["Copy", "Copy"],
        "State": ["CA", "CA"],
        "Price per product": [8.0, 8.0]
    })
    pdf.to_csv(tmp_csv, index=False)

    df = load_raw_products(spark, str(tmp_csv))

    assert df.count() == 2, f"RAW layer should preserve duplicates. Expected 2 rows, got {df.count()}"

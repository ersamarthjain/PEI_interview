import pytest
import pandas as pd
from pyspark.sql import SparkSession
from pei.raw.customers_raw import load_raw_customers


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("customers-raw-tests")
        .master("local[*]")
        .getOrCreate()
    )

def test_load_raw_customers_success(spark, tmp_path):
    """Validate correct load for a normal Excel file."""

    temp_file = tmp_path / "Customer.xlsx"

    pdf = pd.DataFrame({
        "Customer ID": ["C1", "C2"],
        "Customer Name": ["Alice", "Bob"],
        "Country": ["USA", "UK"]
    })
    pdf.to_excel(temp_file, index=False)

    df = load_raw_customers(spark, str(temp_file))

    expected_cols = {"customerid", "customername", "country", "inserttimestamp"}
    assert expected_cols == set(df.columns), f"Expected columns {expected_cols} but got {df.columns}"

    assert df.count() == 2, "Row count mismatch"


def test_load_raw_customers_schema_evolution(spark, tmp_path):
    """Ensure additional columns in the Excel file are preserved."""

    temp_file = tmp_path / "Customer.xlsx"

    pdf = pd.DataFrame({
        "Customer ID": ["C1"],
        "Customer Name": ["Alice"],
        "Country": ["USA"],
        "New Column": ["ExtraValue"]   # schema evolution
    })
    pdf.to_excel(temp_file, index=False)

    df = load_raw_customers(spark, str(temp_file))

    assert "newcolumn" in df.columns, "Schema evolution failed—new column was not preserved"

    row = df.first()
    assert row.newcolumn == "ExtraValue", "New column value mismatch after loading"


def test_column_name_normalization(spark, tmp_path):
    """Ensure column names are lowercased and spaces removed."""

    temp_file = tmp_path / "Customer.xlsx"

    pdf = pd.DataFrame({
        "Customer ID ": ["C1"],
        " Customer Name": ["John  "],
        "Country ": [" India "]
    })
    pdf.to_excel(temp_file, index=False)

    df = load_raw_customers(spark, str(temp_file))

    expected_cols = {"customerid", "customername", "country", "inserttimestamp"}
    assert expected_cols == set(df.columns), f"Column normalization failed. Got {df.columns}"


def test_all_values_cast_to_string(spark, tmp_path):
    """Ensure loader casts all columns to string because pandas astype(str) is used."""

    temp_file = tmp_path / "Customer.xlsx"

    pdf = pd.DataFrame({
        "Customer ID": [101],
        "Customer Name": ["Alice"],
        "Country": ["USA"]
    })
    pdf.to_excel(temp_file, index=False)

    df = load_raw_customers(spark, str(temp_file))
    row = df.first()

    assert isinstance(row.customerid, str), "Customer ID must be casted to string"
    assert row.customerid == "101", "Integer not casted correctly to string"


def test_special_characters(spark, tmp_path):
    """Ensure special characters survive the load (no cleaning in raw layer)."""

    temp_file = tmp_path / "Customer.xlsx"

    pdf = pd.DataFrame({
        "Customer ID": ["C1"],
        "Customer Name": ["J@ne! Doe##"],
        "Country": ["U$A"]
    })
    pdf.to_excel(temp_file, index=False)

    df = load_raw_customers(spark, str(temp_file))
    row = df.first()

    assert row.customername == "J@ne! Doe##", "Special characters should remain unchanged in raw layer"


def test_null_values(spark, tmp_path):
    """Raw layer should preserve null values as strings 'nan' due to pandas astype(str)."""

    temp_file = tmp_path / "Customer.xlsx"

    pdf = pd.DataFrame({
        "Customer ID": ["C1", None],
        "Customer Name": ["Alice", None],
        "Country": ["USA", None]
    })
    pdf.to_excel(temp_file, index=False)

    df = load_raw_customers(spark, str(temp_file))

    row2 = df.collect()[1]

    assert row2.customerid == "None", "None values should be converted to string 'None' in raw layer"


def test_missing_file_error(spark):
    """Ensure FileNotFoundError is raised when file does not exist."""

    with pytest.raises(FileNotFoundError):
        load_raw_customers(spark, "/path/does/not/exist.xlsx")


def test_empty_excel_file(spark, tmp_path):
    """Ensure empty files are still handled without crashing."""

    temp_file = tmp_path / "Customer.xlsx"

    pdf = pd.DataFrame(columns=["Customer ID", "Customer Name", "Country"])
    pdf.to_excel(temp_file, index=False)

    df = load_raw_customers(spark, str(temp_file))

    assert df.count() == 0, "Empty Excel file should produce empty DataFrame"

    expected_cols = {"customerid", "customername", "country", "inserttimestamp"}
    assert expected_cols == set(df.columns), "Empty file columns not normalized correctly"

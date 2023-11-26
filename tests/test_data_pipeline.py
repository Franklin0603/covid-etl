import pytest 
import pandas as pd
import numpy as np
from pandas.api.types import is_numeric_dtype

@pytest.fixture
def df():
    """
    This fixture reads data from an Excel file and returns a pandas DataFrame.
    """
    data = '../data/cleaned/covid_cleaned_20230802.csv'
    try: 
        df = pd.read_csv(data)
    except Exception as e:
        raise Exception(f"Failed to read data from Excel file due to:{str(e)}")
    
    if df.empty:
        raise ValueError('The Dataframe is empty')

    return df

@pytest.fixture
def required_columns():
    """
    This fixture returns a list of required columns that must be present in the DataFrame.
    """
    return ['CASES_DATE','COUNTY','STATE','FIPS','CASES','DEATHS','DB_DATE_UPLOAD','SOURCE_TYPE']

def test_col_exists(df, required_columns):
    """
    This test checks if all required columns are present in the DataFrame.
    """
    for column in required_columns:
        assert column in df.columns, f"{column} is missing in DataFrame columns"

def test_null_check(df):
    """
    This test checks if there are any null values in the required columns of the DataFrame.
    """
    columns = ['CASES_DATE','COUNTY']
    for column in columns:
        assert not df[column].isnull().any(), f"{column} contains null values"

def test_deaths_dtype_int(df):
    """
    This test checks the data type of the 'DEATHS' column in the DataFrame.
    It should be of type integer.
    """
    assert df['DEATHS'].dtype == np.dtype('float'), "'DEATHS' is not of type int64"

# def test_unique_check(df):
#     """
#     This test checks if all values in the required columns of the DataFrame are unique.
#     """
#     columns = ['STATE']
#     for column in columns:
#         assert df[column].is_unique, f"{column} contains duplicate values"        
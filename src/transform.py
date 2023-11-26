# import libraries
import logging
import datetime
from datetime import date

# data manipulation
import pandas as pd
import numpy as np

# import self-created libraries
from logging_utils import setup_logger
logger = setup_logger(__name__)

def convert_column_dict(df, column, to_type):
    """Converts the data type of a specified DataFrame column.

    Parameters:
    df : pandas.DataFrame
        DataFrame containing the column to be converted
    column : str
        Column to be converted
    to_type : str
        Type to convert the column to; one of ['float', 'int', 'str', 'date']

    Raises:
    ValueError
        If to_type is not one of ['float', 'int', 'str', 'date']
    """
    if column not in df.columns:
        raise ValueError(f"Column {column} not found in DataFrame")

    if to_type == 'float':
        df[column] = df[column].astype(float)
    elif to_type == 'int':
        df[column] = df[column].fillna(-1).astype(int)
        df[column] = df[column].replace(-1, np.nan)
    elif to_type == 'str':
        df[column] = df[column].replace(np.nan, 'Unknown').astype(str)
    elif to_type == 'date':
        df[column] = pd.to_datetime(df[column], errors='coerce')
    else:
        raise ValueError("Invalid to_type. Choose from 'float', 'int', 'str', 'date'")

def _fips_cleaner(code):
    """
    Standardizes county FIPS codes as 5-digit strings.

    Parameters:
    - code (pd.Series): A series containing FIPS codes as string, int, or float type.

    Returns:
    - pd.Series: Standardized FIPS codes as 5-digit strings.
    """
    return code.astype(str).str.extract('(^[^/.]*).*', expand=False).str.zfill(5)


def transform_data_local(df: pd.DataFrame):
    
    """
    Transforms NYT county-level COVID data.

    Parameters:
    - df (pd.DataFrame): Input DataFrame containing NYT county-level COVID data.

    Returns:
    - pd.DataFrame: Transformed DataFrame.
    """
    # Cast date as datetime
    convert_column_dict(df, 'date', 'date')
    # Drop records with county = 'Unknown' or no FIPs code
    df = df.loc[(df['county'] != 'Unknown') & (df['fips'].notnull())].copy()
    # Store FIPS codes as standard 5 digit strings
    df['fips'] = _fips_cleaner(df['fips'])
    # Drop FIPs that are not part of US states, cast deaths to int
    df = df.loc[df['fips'].str.slice(0,2) <= '56'].copy()
    convert_column_dict(df, 'deaths', 'int')

    # rename and uppercase header
    df.rename(columns={'date': 'CASES_DATE'}, inplace=True)
    df.columns = [column.upper() for column in df.columns ]

    today_str = date.today()
    df['DB_DATE_UPLOAD'] = today_str
    df['DB_DATE_UPLOAD'] = pd.to_datetime(df['DB_DATE_UPLOAD'])

    df['SOURCE_TYPE'] = 'local'
    df.columns = ['CASES_DATE', 'COUNTY', 'STATE', 'FIPS', 'CASES', 'DEATHS', 'DB_DATE_UPLOAD', 'SOURCE_TYPE']

    return df

def transform_data_github(df):
    
    """
    Transforms NYT county-level COVID data.

    Parameters:
    - df (pd.DataFrame): Input DataFrame containing NYT county-level COVID data.

    Returns:
    - pd.DataFrame: Transformed DataFrame.
    """
    # Cast date as datetime
    convert_column_dict(df, 'date', 'date')
    # Drop records with county = 'Unknown' or no FIPs code
    df = df.loc[(df['county'] != 'Unknown') & (df['fips'].notnull())].copy()
    # Store FIPS codes as standard 5 digit strings
    df['fips'] = _fips_cleaner(df['fips'])
    # Drop FIPs that are not part of US states, cast deaths to int
    df = df.loc[df['fips'].str.slice(0,2) <= '56'].copy()
    convert_column_dict(df, 'deaths', 'int')

    # rename and uppercase header
    df.rename(columns={'date': 'CASES_DATE'}, inplace=True)
    df.columns = [column.upper() for column in df.columns ]

    today_str = date.today()
    df['DB_DATE_UPLOAD'] = today_str
    df['DB_DATE_UPLOAD'] = pd.to_datetime(df['DB_DATE_UPLOAD'])

    df['SOURCE_TYPE'] = 'github'
    df.columns = ['CASES_DATE', 'COUNTY', 'STATE', 'FIPS', 'CASES', 'DEATHS', 'DB_DATE_UPLOAD', 'SOURCE_TYPE']


    return df

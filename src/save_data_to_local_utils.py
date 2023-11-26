# import libraries
import os
import datetime
from datetime import date

# data manipulation
import pandas as pd
import numpy as np

from logging_utils import setup_logger

# Setup logger
logger = setup_logger(__name__)

def save_data(df: pd.DataFrame, 
              base_filename: str = 'covid', 
              source_type: str = '',
              directory: str = '../data/raw_data/') -> None:
    """
    Saves a DataFrame to a CSV file on the local filesystem.

    Parameters
    ----------
    df: pd.DataFrame
        DataFrame to save

    base_filename : str
        Base filename for the CSV file, date will be appended to this

    source_type : str
        The source of the data

    directory : str
        Directory to save the CSV file

    Returns
    -------
    None
    """
     # Ensure directory exists
    os.makedirs(directory, exist_ok=True)

    # Get today's date as a string
    today = datetime.datetime.now().strftime("%Y%m%d")

    # Full path includes the directory, base filename, today's date, and the .csv extension
    full_path = os.path.join(directory, f"{base_filename}_{source_type}_{today}.csv")
    
    try:
        df.to_csv(full_path, index=False)
        logger.info(f"Data saved successfully to {full_path}.")
    except Exception as e:
        logger.error(f"An error occurred while saving the DataFrame to a CSV file: {e}")
        raise

def save_data_source_type(local_source, github_source, df ):
    if local_source:
        save_data(df, base_filename='covid', source_type='localsource', directory='../data/raw_data/')
    elif github_source: 
        save_data(df, base_filename='covid', source_type='githubsource', directory='../data/raw_data/')



# def save_data(df: pd.DataFrame, 
#               base_filename: str = 'covid', 
#               directory: str = 'data/save_data/') -> None:
#     """
#     Saves a DataFrame to a CSV file on the local filesystem.

#     Parameters
#     ----------
#     df: pd.DataFrame
#         DataFrame to save

#     base_filename : str
#         Base filename for the CSV file, date will be appended to this

#     directory : str
#         Directory to save the CSV file

#     Returns
#     -------
#     None
#     """
#      # Ensure directory exists
#     os.makedirs(directory, exist_ok=True)

#     # Get today's date as a string
#     today = datetime.datetime.now().strftime("%Y%m%d")

#     # Full path includes the directory, base filename, today's date, and the .csv extension
#     full_path = directory + base_filename + '_' + today + '.csv'
    
#     try:
#         df.columns = ['CASES_DATE', 'COUNTY', 'STATE', 'FIPS', 'CASES', 'DEATHS', 'DB_DATE_UPLOAD']
#         df.to_csv(full_path, index=False)
#         logger.info(f"Data saved successfully to {full_path}.")
#     except Exception as e:
#         logger.error(f"An error occurred while saving the DataFrame to a CSV file: {e}")
#         raise
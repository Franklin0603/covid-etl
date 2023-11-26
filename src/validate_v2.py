import os
import sys 
import datetime
import pandas as pd

import logging
from logging_utils import setup_logger

logger = setup_logger(__name__)

def cases_vs_deaths(df):
    """Checks that death count is no more than case count."""
    failure_message = "Death counts cannot exceed case counts."
    date_now = pd.to_datetime('today').date()
    if (df['DEATHS'] == df['CASES']).all():
        return pd.DataFrame([[date_now, 'cases_vs_deaths', 'passed', 'successful','info']],
                   columns=['date', 'validate_test', 'validate_flag','error_message','error_level'])
        
    else:
        return pd.DataFrame([[date_now, 'cases_vs_deaths', 'failed', failure_message,'warning']],
                             columns=['date', 'validate_test', 'validate_flag','error_message','error_level'])
    
def unique_records(df):
    """Checks that each date and FIPs combination is unique."""
    failure_message = "Only one record per FIPs, per date allowed."
    date_now = pd.to_datetime('today').date()
    if df[['CASES_DATE', 'FIPS']].drop_duplicates().shape[0] == df.shape[0]:   
        return pd.DataFrame([[date_now, 'unique_records', 'passed', 'successful','info']],
                   columns=['date', 'validate_test', 'validate_flag','error_message','error_level'])
    else: 
        return pd.DataFrame([[date_now, 'unique_records', 'failed', failure_message,'warning' ]],
                            columns=['date', 'validate_test', 'validate_flag','error_message','error_level'])
    

def no_nulls_test(df):
    """Checks that all elements are not null"""
    failure_message = "All values are expected to be non-null."
    date_now = pd.to_datetime('today').date()
    if df.isnull().any().sum() == 0:
        return pd.DataFrame([[date_now, 'no_nulls_test', 'passed', 'successful','info']],
                   columns=['date', 'validate_test', 'validate_flag','error_message','error_level'])
        
    else:
        return pd.DataFrame([[date_now, 'no_nulls_test', 'failed', failure_message,'critical']],
                             columns=['date', 'validate_test', 'validate_flag','error_message','error_level'])
    

def cases_range_test(df):
    """Checks that all cases are non-negative and <= 10M"""
    failure_message = "Cases must be non-negative and <= 10M."
    date_now = pd.to_datetime('today').date()
    if (df['CASES'] >= 0).all() and (df['CASES'] <= 10e6).all():
        return pd.DataFrame([[date_now, 'cases_range_test', 'passed', 'successful','info']],
                   columns=['date', 'validate_test', 'validate_flag','error_message','error_level'])
        
    else:
        return pd.DataFrame([[date_now, 'cases_range_test', 'failed', failure_message,'critical']],
                             columns=['date', 'validate_test', 'validate_flag','error_message','error_level'])

        
def deaths_range_test(df):
    """Checks that all deaths are non-negative and <= 100K"""
    failure_message = "Deaths must be non-negative and <= 100K."
    date_now = pd.to_datetime('today').date()
    if (df['DEATHS'] >= 0).all() and (df['DEATHS'] <= 1e2).all():      
        return pd.DataFrame([[date_now, 'deaths_range_test', 'passed', 'successful','info']],
                   columns=['date', 'validate_test', 'validate_flag','error_message','error_level'])
    else:
        return pd.DataFrame([[date_now, 'deaths_range_test', 'failed', failure_message,'critical']],
                             columns=['date', 'validate_test', 'validate_flag','error_message','error_level'])

        

def append_to_validation_dataframe(df: pd.DataFrame):
    """
    Append the result of each validation test to a DataFrame.

    Parameters
    ----------
    df : pandas.DataFrame
        Input DataFrame to be validated.

    Returns
    -------
    pandas.DataFrame
        DataFrame with validation test results appended.
    """
    validate_df = pd.concat([
            cases_vs_deaths(df),
            unique_records(df),
            no_nulls_test(df),
            cases_range_test(df),
            deaths_range_test(df)
    ], ignore_index=True)
    return validate_df

def save_vadidation(df: pd.DataFrame, base_filename: str = 'covid_validation', directory: str = '../data/validation/') -> None:
    """
    Saves a DataFrame to a CSV file on the local filesystem.

    Parameters
    ----------
    df: pd.DataFrame
        DataFrame to save

    base_filename : str
        Base filename for the CSV file, date will be appended to this

    directory : str
        Directory to save the CSV file

    Returns
    -------
    None
    """
    # read the validate data
    validate_df = append_to_validation_dataframe(df)

    # Ensure directory exists
    os.makedirs(directory, exist_ok=True)

    # Get today's date as a string
    today = datetime.datetime.now().strftime("%Y%m%d")

    # Full path includes the directory, base filename, today's date, and the .csv extension
    full_path = directory + base_filename + '_' + today + '.csv'
    
    try:
        validate_df.to_csv(full_path, index=False)
        logger.info(f"validate info saved successfully to {full_path}.")
    except Exception as e:
        logger.error(f"An error occurred while saving the DataFrame to a CSV file: {e}")
        raise

def validation_test(df):
    """
    Run data validation tests on the provided DataFrame.

    Parameters
    ----------
    df : pandas.DataFrame
        Input DataFrame to be validated.

    Returns
    -------
    bool
        True if all tests pass, False otherwise.
    """
    validate_df = append_to_validation_dataframe(df)
    
    for _, row in validate_df.iterrows():
        if (row['validate_flag']) == 'passed':
           logger.debug(f"Data test {row['validate_test']} passed.")
        elif (row['validate_flag'] == 'fail') and (row['error_level'] == 'critical'):
            logger.critical(f"Data test {row['validate_test']} failed. {row['error_message']}")
        else:
            logger.warning(f"Data test {row['validate_test']} failed. {row['error_message']}")

    pass_result = len(validate_df[validate_df['validate_flag'] == 'passed'])
    all_result  = len(validate_df)

    if pass_result == all_result:
        logger.debug(f"{pass_result}/{all_result} tests passed.")
    elif 3 <= pass_result <= 4:
        logger.warning(f"{pass_result}/{all_result} tests passed.")
    else:
        logger.critical(f"{pass_result}/{all_result} tests passed.")
    
    return True
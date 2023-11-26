# import libraries
import os
import sys 
import yaml
import time
import psutil
import logging
import datetime
from datetime import date

# data manipulation
import pandas as pd
import numpy as np

# import self-created libraries
import database_utils
from logging_utils import setup_logger

# Setup logger
logger = setup_logger(__name__)


def create_dataframe_logs(df, 
                          raw_memory,
                          cpu_used,
                          script_duration,
                          DataName='Covid data',
                          date_column = 'CASES_DATE',
                          groupby_flag = None,
                          amount_column = None,
                          base_filename: str = 'covid_monitor', 
                          directory: str = '../data/monitor_logs/'):
    """
    Creates a log dataframe from input data, saves it to a CSV file, and returns it.
    
    Args:
        df (DataFrame): The source DataFrame.
        raw_memory (int): The raw memory usage.
        cpu_used (int): The CPU usage.
        script_duration (float): The script duration.
        DataName (str, optional): The name of the data. Defaults to 'Covid data'.
        date_column (str, optional): The date column name. Defaults to 'CASES_DATE'.
        groupby_flag (list, optional): A list to group the data by. Defaults to None.
        amount_column (str, optional): The amount column name. Defaults to None.
        base_filename (str, optional): The base filename for the CSV file. Defaults to 'covid'.
        directory (str, optional): The directory path. Defaults to 'data/database_logs_data/'.
    
    Returns:
        DataFrame: The log DataFrame.
    """

    

    try:
        row_count = '{:,}'.format(len(df))

        # Calculate first and latest date 
        if date_column in df.columns:
            df[date_column] = pd.to_datetime(df[date_column])
            first_date = df[date_column].min()
            latest_date = df[date_column].max()
        else:
            first_date, latest_date = None, None 

        # Count the unique records in the dataframe
        unique_records = "{:,}".format(len(df[['COUNTY','STATE']].drop_duplicates()))

        # Missing values per column
        missing_values_count = df.isnull().sum().sum()

         # Calculate total, average, and median amount if amount_column is provided
        if amount_column in df.columns:
            total_amount =  "$ {:,}".format(round(df[amount_column].sum(),2))
            avg_amount =    "$ {:,}".format(round(df[amount_column].mean(),2))
            median_amount = "$ {:,}".format(round(df[amount_column].median(),2))
        else:
            total_amount, avg_amount, median_amount = None, None, None

        if groupby_flag is not None and set(groupby_flag).issubset(df.columns):
            change_over_time = df.groupby(groupby_flag[0])[groupby_flag[1]].mean()
        else:
            change_over_time = None


        # Calculate the most frequent value for each column
        most_frequent = df.mode().iloc[0]

        # Create a dataframe to hold the log metrics
        logs_entry = pd.DataFrame([[DataName, row_count, unique_records, missing_values_count, total_amount, 
                                    avg_amount, median_amount, first_date, latest_date, raw_memory, cpu_used, 
                                    script_duration, date.today()]],
                                    columns=['DATA_NAME', 'ROW_COUNT', 'UNIQUE_RECORD', 'MISSING_VALUES_COUNT', 
                                             'TOTAL_AMOUNT', 'AVERAGE_AMOUNT', 'MEDIAN_AMOUNT', 'DATA_FIRST_DATE', 
                                             'DATA_LATEST_DATE','RAW_MEMORY', 'CPU_MEMORY', 'SCRIPT_DURATION', 'LOG_UPLOAD_DATE'])

        logs_entry.dropna(axis=1, how='any', inplace=True) 

        # Ensure directory exists
        os.makedirs(directory, exist_ok=True)

        full_path = f"{directory}{base_filename}_logs.csv"

        try: 
            if not os.path.isfile(full_path):
                logs_entry.to_csv(full_path, index=False,)
            else: 
                logs_entry.to_csv(full_path, mode='a', header=False, index=False,)
            logger.info(f"Data saved successfully to {base_filename}_logs.csv .")
        except pd.errors.ParserError as e:
            logger.info(f"An error occurred while saving the DataFrame to a CSV file: {e}")
            raise
        except Exception as e:
            logger.error("An unknown error occurred:", e)
            raise
    
        return logs_entry

    except Exception as e:
        print("An error occurred:", e)

def load_logs_in_database(dataframe_logs, 
                          database_type_flag = "local",  
                          db_name = 'covid_db',
                          table_name = 'covid_logs'):
  
    try:
        credentials = database_utils.yaml_credentials(database_type_flag) 
        connection = database_utils.connect_db(credentials)  
        
        if connection is not None:
            # load sale data into local database
            database_utils.ensure_database_exists(connection, db_name)

            create_table_query = f"""
                    CREATE TABLE IF NOT EXISTS {db_name}.{table_name} (
                        DATA_NAME varchar(255),
                        ROW_COUNT varchar(255),
                        UNIQUE_RECORD varchar(255),
                        MISSING_VALUES_COUNT INT,
                        DATA_FIRST_DATE DATE,
                        DATA_LATEST_DATE DATE,
                        RAW_MEMORY varchar(255),
                        CPU_MEMORY varchar(255),
                        SCRIPT_DURATION varchar(255),
                        LOG_UPLOAD_DATE DATE)
                    """
            database_utils.create_table(connection, create_table_query, db_name, table_name)
            # load logs info into local database
            database_utils.load_data_to_table(dataframe_logs, credentials, db_name, table_name, chunksize=10000)

            connection.close()
    except Exception as e:
        logger.error(f"An error occurred while trying to connect to the database: {str(e)}")
        raise
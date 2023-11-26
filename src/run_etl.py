# import libraries
import os
import sys 
import glob
import shutil
import yaml
import time
import psutil
import logging
import argparse
import urllib.parse
import datetime
from datetime import date

# data manipulation
import pandas as pd
import numpy as np

# import self-created libraries
import extract
import src.transform as transform
import src.validate as validate
import load
import database_utils
import src.monitor_logs as monitor_logs
import email_utils
from logging_utils import setup_logger

# Setup logger
logger = setup_logger(__name__)

def main(table_source, data_source, local_source, validate_data, options):
    """
    Executes ETL pipeline for a single table.

    Parameters:
    - table_source (str): Fully-qualified table for load into bit.io.
    - data_source (str): URL for source data extraction or path to a local CSV file.
    - local_source (bool): True if `data_source` is a path to a local CSV file, False if it's a URL.
    - validate_data (bool): True if data validation should be run, False otherwise.
    - options (dict): Option-argument map from the user command.
    """
    
    script_start_time = time.time()

    # EXTRACT DATA
    start_time = time.time()

    logger.info('=> STARTING EXTRACT...')
    if local_source:
        df = extract.csv_from_local(data_source)
        if not df.empty:
            logger.info('Done reading CSV data from local.')
        else:
            logger.critical('Dataframe is emply')
            sys.exit(1)

    else:
        df = extract.csv_from_get_request(data_source)
        if not df.empty:
            logger.info('Done extracting data from the URL.')
        else:
            logger.critical('URL data is empty')
            sys.exit(1) 
    
    time_duration = time.time() - start_time
    cpu_procress_extract = psutil.cpu_percent()
    logger.info(f'Extract process for Covid - CPU usage {cpu_procress_extract}%')
    logger.info(f'Extract process for Covid - function took : {round(time_duration,2)} seconds')


    # TRANSFORM DATA
    start_time = time.time()

    logger.info('=> STARTING TRANSFORM...')
    if 'name' in options:
        transform_func = getattr(transform, options['name'], None)
        if transform_func:
            logger.info(f"Starting transform with {options['name']}...")
            df = transform_func(df)
            logger.info(f"Done transforming {options['name']}.")
        else:
            logger.critical("Specified transformation name not found")
            raise ValueError("Specified transformation name not found.")
    else:
        logger.critical("No transformation specified, skipping to validation step.")

    time_duration = time.time() - start_time
    cpu_procress_transform = psutil.cpu_percent()
    logger.info(f'Transform process for Covid - CPU usage {cpu_procress_transform}%')
    logger.info(f'Transform process for Covid - function took : {round(time_duration,2)} seconds')
    

    # VALIDATE DATA
    start_time = time.time()

    logger.info('=> STARTING VALIDATE...')
    if ('name' in options) and validate_data:
        logger.info(f"Starting data validation with {options['name']}...")
        validate.nyt_cases_counties_validate_check(df)
        validate.save_vadidate_info(df)
    else:
        logger.warning(f"No data validation specified, skipping to load step.")  

    time_duration = time.time() - start_time
    cpu_procress_validate = psutil.cpu_percent()
    logger.info(f'Validation process for Covid - CPU usage {cpu_procress_validate}%')
    logger.info(f'Validation process for Covid - function took : {round(time_duration,2)} seconds')
    
    # LOAD DATA
    start_time = time.time()

    logger.info('=> STARTING LOAD...')
    logger.info(f"Saving {options['name']} data to local...")
    # load.save_data(df, 'data/save_data/covid-test.csv')
    load.save_data(df)
    load.load_data_in_database(df)
    logger.info(f"{options['name']} data saved to local.")

    time_duration = time.time() - start_time
    cpu_procress_load = psutil.cpu_percent()
    logger.info(f'Load process for Covid - CPU usage {cpu_procress_load}%')
    logger.info(f'Load process for Covid - function took : {round(time_duration,2)} seconds')

    # LOAD LOGS IN DATABASE
    
    #calculate resource usage
    raw_memory = "{:} %".format(psutil.virtual_memory().percent)
    cpu_used = "{:} %".format(psutil.cpu_percent())
    script_duration = round(time.time() - script_start_time,2)
    script_duration = "{:} sec".format(script_duration)

    logger.info('=> STARTING CREATE DATA LOGS...')
    logger.info(f"Starting create data logs")
    logs_entry = monitor_logs.create_dataframe_logs(df, raw_memory, cpu_used, script_duration)
    monitor_logs.load_logs_in_database(logs_entry)
    logger.info(f"Done creating data logs")

    # SESSION SUMMARY
    logger.info('=> SESSION SUMMARY...') 
    raw_memory = psutil.virtual_memory().percent
    cpu_used = ((cpu_procress_extract + cpu_procress_transform + cpu_procress_validate + cpu_procress_load)/4)
    script_duration = time.time() - script_start_time
    logger.info(f'RAM memory {raw_memory}% used')
    logger.info(f'Average CPU usage {cpu_used}%')
    logger.info(f"Multiple threads took : {round(script_duration,2)} seconds")

    # SEND DATA VALIDATION EMAIL
    logger.info('=> SEND DATA VALIDATION EMAIL...') 
    
   # move latest data files to the destination folder
    source_folders = ["data/validate_info_data", "data/log_info_data"]
    destination_folder = "data/email-data"
    for folder in source_folders:
        email_config.copy_latest_files(folder, destination_folder)
    
    # Convert log files to text
    log_directory = os.path.join(os.getcwd(), 'data/email-data')
    email_config.process_log_files(log_directory)
    email_config.delete_log_files(log_directory)

    # send the email
    email_config.send_emails_data_validate(smtp_port = 587, smtp_server = "smtp.gmail.com")
    email_config.delete_files(destination_folder)


def print_help():
    help_text = """
    Usage:
        python main.py [table_source] [data_source] [--local-source] [--validate-data]
        
    Example:
        python main.py nyt_cases_counties 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv' --local-source --validate-data
    """
    return help_text

if __name__ == '__main__':
    

    # Parse command line options and arguments
    logger.info('=> COVID PROJECT <=')
    # Instantiate the parser
    parser = argparse.ArgumentParser(description='This script is used to process Covid Data.', 
                                     epilog=print_help(), 
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    # Add arguments
    parser.add_argument('table_source', type=str, help="Table source: nyt_cases_counties")
    parser.add_argument('data_source', type=str, help="Data source: https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv ")
    parser.add_argument('--local-source', dest='local_source', action='store_true',
                        help='Specify this flag to indicate that the source is local.')
    parser.add_argument('--validate-data', dest='validate_data', action='store_true',
                        help='Specify this flag to indicate that data should be validated.')
    # Setting default values
    parser.set_defaults(local_source=False, validate_data=False)
    
    # Parse arguments
    args = parser.parse_args()
    option_args = {'name': args.table_source}

    # Execute ETL
    logger.info(f'Starting ETL for Covid Data...')
    main(args.table_source, args.data_source, args.local_source, args.validate_data, option_args)
    logger.info('Done running ETL for Covid Data')
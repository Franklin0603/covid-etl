import os
import sys
import time
import psutil
import argparse

# import self-created packages
import extract
import transform
import validate
import load
import monitor_logs
import email_utils
import save_data_to_local_utils as save_data
from logging_utils import setup_logger

# Setup logger
logger = setup_logger(__name__)

def run_etl(data_source, local_source, github_source, save_data_flag, transform_data, validate_data):
    """
    Executes ETL pipeline for a single table.

    Parameters:
    - table_source (str): Fully-qualified table for load into bit.io.
    - data_source (str): URL for source data extraction or path to a local CSV file.
    - local_source (bool): True if `data_source` is a path to a local CSV file, False if it's a URL.
    - validate_data (bool): True if data validation should be run, False otherwise.
    - options (dict): Option-argument map from the user command.
    """
    # Script run time and memory use
    script_start_time = time.time()

    # EXTRACT DATA
    if local_source:
        df = extract.csv_from_local(data_source)
        if not df.empty:
            logger.info('Done reading CSV data from local')
        else:
            logger.critical('Dataframe is emply')
            sys.exit(1)
    elif github_source:
        df = extract.csv_from_get_request(data_source)
        if not df.empty:
            logger.info('Done extracting data from the gittub URL.')
        else:
            logger.critical('URL data is empty')
            sys.exit(1)

    if save_data_flag: 
        save_data.save_data_source_type(local_source, github_source, df )

    # TRANSFORM DATA
    if transform_data:
        if local_source and transform_data:
            logger.info(f"Starting transform with localsource data...")
            df_transform = transform.transform_data_local(df)
            logger.info(f"Done transforming localsource data.")
        elif github_source and transform_data:
            logger.info(f"Starting transform with github source data...")
            df_transform = transform.transform_data_github(df)
            logger.info(f"Done transforming github source data.")
    else:
        logger.error("No transformation specified, skipping to validation step.") 

    # VALIDATE DATA

    logger.info('=> STARTING VALIDATE...')
    if local_source and transform_data and validate_data:
        logger.info(f"Starting data validation with localsource data...")
        validate.validation_test(df_transform)
        validate.save_vadidation(df_transform)
    else:
        logger.error(f"No data validation specified, skipping to load step.")
    
    # LOAD DATA
    logger.info('=> STARTING LOAD...')
    logger.info(f"Saving local source data to local...")
    load.load_data_in_database(df_transform)
    save_data.save_data(df_transform, base_filename = 'covid', source_type ='cleaned' , directory ='../data/cleaned/')
    logger.info(f"local source data saved to local.")

    #calculate resource usage
    raw_memory = "{:} %".format(psutil.virtual_memory().percent)
    cpu_used = "{:} %".format(psutil.cpu_percent())
    script_duration = round(time.time() - script_start_time,2)
    script_duration = "{:} sec".format(script_duration)

    # LOAD LOGS IN DATABASE
    logger.info('=> STARTING CREATE DATA LOGS...')
    logger.info(f"Starting create data logs")
    logs_monitor_entry = monitor_logs.create_dataframe_logs(df_transform, raw_memory, cpu_used, script_duration)
    monitor_logs.load_logs_in_database(logs_monitor_entry)
    logger.info(f"Done creating data logs")

    # SEND DATA VALIDATION EMAIL
    logger.info('=> SEND DATA VALIDATION EMAIL...') 
    
    # move latest data files to the destination folder
    source_folders = ["../data/validation", "../data/logs"]
    destination_folder = "../data/data_for_email"
    for folder in source_folders:
        email_utils.copy_latest_files(folder, destination_folder)

    # Convert log files to text
    log_directory = '../data/data_for_email'
    email_utils.process_log_files(log_directory)
    email_utils.delete_log_files(log_directory)

    # send the email
    email_utils.send_emails_data_validate(smtp_port = 587, smtp_server = "smtp.gmail.com")
    email_utils.delete_files(destination_folder)

def print_help():
    help_text = """
    Example:
        [pull data from local] ==> python test_run_elt.py --local-source 
        [pull data from github and validate] ==> python test_run_elt.py 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv' --github-source --validate-data
        
    """
    return help_text

if __name__ == '__main__':
    print('=> COVID ETL PIPELINE <=')
    parser = argparse.ArgumentParser(description="This script is used to process Covid Data by using ETL process.",
                                     epilog=print_help(), 
                                     formatter_class=argparse.RawDescriptionHelpFormatter)

    # Add arguments
    parser.add_argument('data_source', 
                        type=str, 
                        help="Data source: https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv")
    
    
    parser.add_argument('--local-source', 
                        dest='local_source', 
                        action='store_true',
                        help='Specify this flag to indicate that the source is local.')
    
    parser.add_argument('--github-source', 
                        dest='github_source', 
                        action='store_true',
                        help='Specify this flag to indicate that the source is github.')
    
    parser.add_argument('--save-data', 
                        dest='save_data', 
                        action='store_true',
                        help='Specify this flag to indicate to save data to local.')
    
    parser.add_argument('--transform', 
                        dest='transform', 
                        action='store_true',
                        help='Specify this flag to indicate to transform the data.')
    

    parser.add_argument('--validate', 
                        dest='validate', 
                        action='store_true',
                        help='Specify this flag to indicate to validate the data.')
    
    # Setting default values
    parser.set_defaults(local_source=False, 
                        validate=False, 
                        save_data=False,
                        github_source=False,
                        transform=False)
    
    args = parser.parse_args()

    # Execute ETL
    print(f'Starting ETL for Covid Data...')
    run_etl(args.data_source, args.local_source, args.github_source, args.save_data, args.transform, args.validate)
    print('Done running ETL for Covid Data')

#python test_run_etl.py -local_source nyt_cases_counties 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv'


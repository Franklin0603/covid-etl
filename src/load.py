# import libraries
import os
import sys 
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
logger = setup_logger(__name__)

def load_data_in_database(df, database_type_flag = "local", db_name = 'covid_db',  table_name = 'covid'):

    credentials = database_utils.yaml_credentials(database_type_flag) 
    connection = database_utils.connect_db(credentials)

    if connection is not None:
        database_utils.ensure_database_exists(connection, db_name)

        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {db_name}.{table_name} (
             CASES_DATE DATE,
             COUNTY varchar(255),
             STATE varchar(255),
             FIPS  INT,
             CASES INT,
             DEATHS INT,
             DB_DATE_UPLOAD DATE NOT NULL, 
             SOURCE_TYPE varchar(255) )
        """
        database_utils.create_table(connection, create_table_query, db_name, table_name)
        database_utils.load_data_to_table(df, credentials, db_name, table_name, chunksize=10000)
        connection.close()

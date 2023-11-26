# import libraries
# import os
import sys 
import yaml
# import time
# import psutil
# import logging
import urllib.parse
# import datetime
# from datetime import date

# import self-created libraries
from logging_utils import setup_logger

# data manipulation
import pandas as pd
import numpy as np

# sql connection
import mysql.connector as mysql
from mysql.connector import Error
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

logger = setup_logger(__name__)

def yaml_credentials(database_type_flag="local"):
    """
    Get the database credentials from a YAML file

    Args:
        database_type_flag (str): Database type. It could be 'local', 'aws' or 'gcp'. Default is 'local'.

    Returns:
        dict: Database credentials
    """

    # ensure database type is valid
    valid_database_types = ['local', 'aws','gcp']
    if database_type_flag not in valid_database_types:
        logger.critical(f"Invalid database_type_flag. Expected one of: {valid_database_types}")
        #print(f"Invalid database_type_flag. Expected one of: {valid_database_types}")
    
    # load database credentials from file
    try:
        with open('./config/credentials.yml') as f:
            credentials_data = yaml.safe_load(f)
    except Exception as e:
        logger.critical(f'Failed to open credentials file: {e}')
        #print(f'Failed to open credentials file: {e}')
        sys.exit(1)

    credentials = credentials_data.get(f'{database_type_flag}_database')

    # ensure port is integer for non-loacal databases
    if credentials.get('port') is not None:
        try:
            credentials['port'] = int(credentials['port'])
        except ValueError:
            logger.critical("Error: Port number must be an integer") 
            #print("Error: Port number must be an integer") 
            sys.exit(1)
    
    return credentials

def connect_db(credentials):
    """
    Connect to a MySQL database

    Args:
        credentials (dict): Credentials to connect to the MySQL database.

    Returns:
        connection (object): MySQL database connection
    """
    
    host_input, user_input, password_input,port_input = credentials["hostname"], credentials["username"],\
                                                        credentials["password"],credentials["port"]

    credentials_info = {"host": host_input,
                        "user": user_input,
                        "password": password_input,
                        "port": port_input}
    
    try:
        connection = mysql.connect(**credentials_info)
        if connection.is_connected():
            logger.info("Connected to MySQL database...")
        return connection
    except mysql.Error as e:
        logger.error(f"Failed to connect to MySQL database: {e}")
        #print(f"Failed to connect to MySQL database: {e}")
        return None
    
def drop_database(connection, db_name):
    """
    Drop a database.
    
    Parameters:
    connection (object): The database connection object.
    db_name (str): The database name to be created.
    
    Returns:
    None
    """
    cursor = connection.cursor()

    try:
        cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")
        logger.info(f"{db_name} database is dropped.")
    except Error as e:
       logger.error(f"Error while dropping database {db_name}", e)
    finally:
        cursor.close()

def drop_table(connection, db_name, table_name):
    """
    Drop a table in the given database
    
    Parameters:
    connection (object): The database connection object.
    db_name (str): The database name.
    table_name (str): The table name to be created.
    
    Returns:
    None
    """
    
    # Drop table SQL query
    drop_table_query=f"""DROP TABLE IF EXISTS {db_name}.{table_name}"""
    cursor = connection.cursor()
    
    try:
        cursor.execute(drop_table_query)
        logger.info(f"{table_name} table is drop in {db_name}")
        #print(f"Error while dropping table {table_name} in {db_name}", e)
    except Error as e:
        logger.error(f"Error while dropping table {table_name} in {db_name}", e)
        #print(f"Error while dropping table {table_name} in {db_name}", e)
    finally:
        cursor.close()
        
def truncate_table(connection, db_name, table_name):
    """
    Truncate a table in the given database
    
    Parameters:
    connection (object): The database connection object.
    db_name (str): The database name.
    table_name (str): The table name to be created.
    
    Returns:
    None
    """
    
    # Drop table SQL query
    truncate_table_query=f"""TRUNCATE TABLE {db_name}.{table_name}"""
    cursor = connection.cursor()
    
    try:
        cursor.execute(truncate_table_query)
        logger.info(f"All rows have been removed from {table_name} in {db_name}")
    except Error as e:
        logger.error(f"Error while romoving rows from {table_name} in {db_name}", e)
    finally:
        cursor.close()    

def ensure_database_exists(connection, db_name):
    """
    Ensure a database exists in MySQL. If not, create it.

    Args:
        connection (object): MySQL database connection
        db_name (str): Name of the database

    Returns:
        None
    """
    cursor = connection.cursor()
    try:
        cursor.execute("SHOW DATABASES")
        databases = [db[0] for db in cursor.fetchall()]

        # If database doesn't exist, create it
        if db_name not in databases:
            cursor.execute(f"CREATE DATABASE {db_name}")
            logger.info(f"{db_name} database is created.")
        else:
            logger.info(f"{db_name} database already exists.")

    except Error as e:
        logger.error(f"Failed to create database {db_name}: {e}")

    finally:
        cursor.close()

def create_table(connection, create_table_query, db_name, table_name):
    """
    Ensure a table exists in a MySQL database. If not, create it.

    Args:
        connection (object): MySQL database connection
        db_name (str): Name of the database
        table_name (str): Name of the table

    Returns:
        None
    """
    cursor = connection.cursor()
    try:
        cursor.execute(create_table_query)
        logger.info(f"{table_name} table is created in {db_name}")
        #print(f"{table_name} table is created in {db_name}")

    except Error as e:
        logger.error(f"Failed to create table {table_name} in {db_name}: {e}")
        #print(f"Failed to create table {table_name} in {db_name}: {e}")

    finally:
        cursor.close()        

def load_data_to_table(df, credentials, db_name, table_name, chunksize=10000):
    """
    Load data from a DataFrame to a MySQL table

    Args:
        df (DataFrame): Data to load
        credentials (dict): MySQL credentials
        db_name (str): Name of the database
        table_name (str): Name of the table
        chunksize (int): Rows to insert at a time

    Returns:
        None
    """
    
    password = urllib.parse.quote_plus(credentials['password'])  # URL encode the password
    engine = create_engine(f"mysql+mysqlconnector://{credentials['username']}:{password}@{credentials['hostname']}:{credentials['port']}/{db_name}")

    try:
        df.to_sql(table_name, con=engine, index=False, if_exists='append', chunksize=chunksize)
        logger.info(f"Data has been loaded to {db_name}.{table_name}")
    except Exception as e:
        logger.critical(f"Failed to load data to {db_name}.{table_name}: {e}")
        


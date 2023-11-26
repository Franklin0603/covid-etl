import io
import pandas as pd
import requests
from logging_utils import setup_logger

logger = setup_logger(__name__)

def csv_from_get_request(url: str) -> pd.DataFrame:
    """
    Extracts data from a GET request and parses the data into a DataFrame

    Parameters
    ----------
    url: str
        URL endpoint to extract the CSV data from

    Returns
    -------
    pd.DataFrame:
        DataFrame constructed from the CSV data or an empty DataFrame if extraction fails
    """
    try:
        r = requests.get(url)
        r.raise_for_status()
        data = r.content.decode('utf-8')
        df = pd.read_csv(io.StringIO(data), low_memory=False)
        logger.info("Data extracted successfully from the URL.")
        return df
    except requests.exceptions.RequestException as e:
        logger.error(f"An error occurred during the GET request: {e}")
        raise


def csv_from_local(path: str) -> pd.DataFrame:
    """
    Extracts a CSV file from the local filesystem and parses the data into a DataFrame.

    Parameters
    ----------
    path : str
        Path to the local CSV file.

    Returns
    -------
    pd.DataFrame:
        DataFrame constructed from the CSV file or an empty DataFrame if file reading fails
    """
    try: 
        df = pd.read_csv(path)
        logger.info("Data read successfully from the local CSV file.")
        return df
    except FileNotFoundError:
        logger.error(f"File not found at path: {path}")
        raise
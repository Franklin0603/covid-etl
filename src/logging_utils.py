import logging
import datetime
import os

def setup_logger(name, level=logging.DEBUG):
    """
    Creates a logger object for logging.

    Parameters:
    - name (str): Name of the logger.
    - level (int): Logging level. Default is DEBUG.

    Returns:
    - logging.Logger: Logger object for logging.
    """
    # Get the current date and format it as a string
    today = datetime.datetime.now().strftime("%Y%m%d")

    # Create a directory if it doesn't exist
    log_directory = '../data/logs/'
    os.makedirs(log_directory, exist_ok=True)

    # Filename includes the current date
    log_file = log_directory + f'covid_logs_{today}.log'

    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(level)

        formatter = logging.Formatter('[%(asctime)s] %(levelname)4s : %(message)s (%(filename)s:%(lineno)s)',
                                      datefmt='%Y-%m-%d %H:%M:%S')

        # Create a file handler
        fh = logging.FileHandler(log_file, mode='a')
        fh.setLevel(level)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger

# import libraries
import os
import sys
import glob
import shutil
import yaml
import csv
import datetime

# import self-created libraries
from logging_utils import setup_logger

# email libraries
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

# Setup logger
logger = setup_logger(__name__)

def delete_files(directory='../data/data_for_email/'):
    """
    Delete all files in a directory.

    Args:
        directory (str): Path to the directory containing the files.

    Returns:
        None
    """
    try:
        files = glob.glob(f"{directory}/*.*")
        for file in files:
            os.remove(file)

        logger.info("Files in the directory deleted successfully.")

    except FileNotFoundError:
        logger.error(f"Directory '{directory}' not found.")
    except Exception as e:
        logger.error("An error occurred while deleting the files: ")
        logger.error(e)

def copy_latest_files(source_folder, destination_folder):
    """
    Copy the latest saved data files from the source folder to the destination folder.

    Args:
        source_folder (str): Path to the source folder.
        destination_folder (str): Path to the destination folder.

    Returns:
        None
    """
    try:
        os.makedirs(destination_folder, exist_ok=True)

        # Find the latest data file in the source folder
        latest_file = max(glob.glob(os.path.join(source_folder, "*.*")), key=os.path.getctime)

        # Move the latest file to the destination folder
        # shutil.move(latest_file, destination_folder)
        shutil.copy(latest_file, destination_folder)

        logger.info(f"{latest_file} copied to the {destination_folder} successfully.")

    except FileNotFoundError:
        logger.error("Source or destination folder not found.")
    except ValueError:
        logger.error("No files found in the source folder.")
    except Exception as e:
        logger.error("An error occurred while copying the latest data files:")
        logger.error(e)



def convert_log_to_text(log_file, text_file):
    """
    Convert a log file to a text file.

    Args:
        log_file (str): Path to the log file.
        text_file (str): Path to the text file to be created.

    Returns:
        None
    """
    try:
        with open(log_file, 'r') as f:
            log_data = f.read()

        with open(text_file, 'w') as f:
            f.write(log_data)

        logger.info(f"Log file '{log_file}' converted to text file '{text_file}' successfully.")

    except FileNotFoundError:
        logger.error(f"Log file '{log_file}' not found.")
    except Exception as e:
        logger.error(f"An error occurred while converting the log file '{log_file}' to text file '{text_file}':")
        logger.error(e)


def process_log_files(directory='../data/data_for_email/'):
    """
    Process log files in a directory by converting them to text files.

    Args:
        directory (str): Path to the directory containing the log files.

    Returns:
        None
    """
    try:
        log_files = glob.glob(f"{directory}/*.log")

        for log_file in log_files:
            text_file = f"{os.path.splitext(log_file)[0]}.txt"
            convert_log_to_text(log_file, text_file)

        logger.info("Log files processed successfully.")

    except FileNotFoundError:
        logger.error(f"Directory '{directory}' not found.")
    except Exception as e:
        logger.error("An error occurred while processing log files:")

def delete_log_files(directory='../data/data_for_email/'):
    """
    Delete log files in a directory.

    Args:
        directory (str): Path to the directory containing the log files.

    Returns:
        None
    """
    try:
        log_files = glob.glob(f"{directory}/*.log")

        for log_file in log_files:
            os.remove(log_file)

        logger.info("Log files deleted successfully.")

    except FileNotFoundError:
        logger.error(f"Directory '{directory}' not found.")
    except Exception as e:
        logger.error(f"An error occurred while deleting log files:{e}")


def yaml_credentials():
    """
    Get the email credentials from a YAML file

    Returns:
        dict: Email credentials
    """
    valid_email_types = ['email_detail']
    email_detail = 'email_detail'

    if email_detail not in valid_email_types:
        raise ValueError(f"Invalid valid_email_types. Expected one of: {valid_email_types}")

    try:
        with open('./config/credentials.yml') as f:
            credentials_data = yaml.safe_load(f)
    except Exception as e:
        logger.error(f'Failed to open credentials file: {e}')
        sys.exit(1)

    credentials = credentials_data.get(f'{email_detail}')

    return credentials


def get_contacts(filename):
    """
    Retrieves the contacts from a file.

    Args:
        filename (str): Name of the contact file.

    Returns:
        tuple: A tuple containing two lists - names and emails.
    """
    names = []
    emails = []

    with open(filename, mode='r', encoding='utf-8') as contacts:
        csv_reader = csv.reader(contacts)
        header = next(csv_reader)

        for row in csv_reader:
            if row[2].lower() == 'true':
                names.append(row[0])
                emails.append(row[1])

    return names, emails


def get_attachment_files(directory='../data/data_for_email/'):
    """
    Retrieves the list of attachment files in the specified directory.

    Args:
        directory (str): Path to the directory containing attachment files.

    Returns:
        list: List of attachment filenames.
    """
    try:
        attachment_files = glob.glob(f"{directory}/*.txt")
    except Exception as e:
        print("attachment_files is empty {e}")
        attachment_files = []

    return attachment_files


def validate_check_info(validate_file):
    validate_test = []
    validate_flag = []
    error_messages = []

    with open(validate_file, 'r') as validate_info:
        csv_reader = csv.DictReader(validate_info)
        for row in csv_reader:
            validate_test.append(row['validate_test'])
            validate_flag.append(row['validate_flag'])
            error_messages.append(row['error_message']) 

    pass_flag = validate_flag.count('passed')
    all_results = len(validate_flag)

    return validate_test, validate_flag, error_messages, pass_flag, all_results


def send_emails_data_validate(smtp_port = 587, smtp_server = "smtp.gmail.com"):
    
    """
    Sends emails with attachments to the specified email addresses.
    """

    today = datetime.datetime.now().strftime("%Y-%m-%d")
    subject = f"Data validation for Covid Report - {today}"

    try:
        credentials = yaml_credentials()
        sender_email, password = credentials['email'], credentials['password']

        # Connect with the server
        with smtplib.SMTP(smtp_server, smtp_port) as smtp_server:
            smtp_server.starttls()
            smtp_server.login(sender_email, password)
            logger.info("Successfully connected to the server")

            # attachment_directory = os.path.join(os.getcwd(), 'data/email-data')
            attachment_directory =  '../data/data_for_email'
            attachment_filenames = get_attachment_files(attachment_directory )

            contact_list_filename = '../contact-list.csv'
            names, emails = get_contacts(contact_list_filename)

            # validate_file = os.path.join(os.getcwd(), "data/email-data/*validate*")
            validate_file = "../data/data_for_email/"
            validate_files = glob.glob(f"{validate_file}/*val*")
            validate_file_path_string = os.path.pathsep.join(validate_files)
            validate_test, validate_flag, error_messages, pass_flag, all_results = validate_check_info(validate_file_path_string)

            for name, email in zip(names, emails):
                html_email = f"""
                        <html>
                            <body>
                                <h3>Hi, {name}</h3>
                                <p>Data validation tests for covid pipeline: {pass_flag}/{all_results} tests passed.</p>
                                <ul>
                """

                for test, flag, message in zip(validate_test, validate_flag, error_messages):
                    html_email += f"<li>Data test for {test}: {flag} - {message}</li>"

                html_email += """
                                </ul>
                                <br/><br/>
                                Here is the link to the Monitor dashboard:  <a href="https://yangzhou1993.medium.com/">Monitor Dashboard</a>
                                <br/><br/>
                                Best Regards,
                                <br/>
                                Data Engineering Team 
                                </p>
                                <br/><br/>
                                <font color="red">Please do not reply to this email as it is auto-generated. </font>
                            </body>
                        </html>
                        """

                msg = MIMEMultipart()
                msg["From"] = sender_email
                msg["To"] = email
                msg["Subject"] = subject

                # Attach the body of the message
                msg.attach(MIMEText(html_email, 'html'))

                # Attach multiple files
                if len(attachment_filenames) > 0:
                    for attachment_filename in attachment_filenames:
                        with open(attachment_filename, 'rb') as attachment:
                            attachment_package = MIMEBase('application', 'octet-stream')
                            attachment_package.set_payload(attachment.read())
                            encoders.encode_base64(attachment_package)
                            filename = attachment_filename.split('/')[-1]
                            attachment_package.add_header('Content-Disposition', f"attachment; filename={filename}")
                            msg.attach(attachment_package)
                else:
                    logger.warnings("There are no attachments")

                # Send the email
                smtp_server.send_message(msg)
                logger.info(f"The email has been sent")

    except Exception as e:
        logger.error("An error occurred while sending emails:")
        logger.error(e)

def main():
    """
    Main function to send emails.
    """
    
    # Move latest data files to the destination folder
    source_folders = ["../data/validation", "../data/logs"]
    destination_folder = '../data/data_for_email/'
    # Move latest data files to the destination folder
    for folder in source_folders:
        copy_latest_files(folder, destination_folder)
    
    # Convert log files to text
    log_directory = '../data/data_for_email/'
    process_log_files(log_directory)
    
    # Delete log files
    delete_log_files(log_directory)
    
    # SMTP configuration
    smtp_port = 587  # Standard secure SMTP port
    smtp_server = "smtp.gmail.com"  # Google SMTP Server
    send_emails_data_validate(smtp_port=587, smtp_server="smtp.gmail.com")
    



if __name__ == "__main__":
    main()
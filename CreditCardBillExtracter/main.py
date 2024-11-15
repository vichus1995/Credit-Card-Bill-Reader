import datetime
import sys

import pyodbc

from CreditCardBillExtracter import config as cf
import utils as ut
from simplegmail import Gmail
from read_emails import download_attachments
from read_pdf import read_pdf
from sql_crud import update_or_insert_credit_card_table, update_watermark
from datetime import datetime, timezone
import logging_config
import logging
from pypdf.errors import PdfReadError


logger = logging.getLogger(__name__)


def main():

    """ Create data, data/input, and data/output folders if they do not exist"""
    try:
        ut.create_dir_if_not_exists(cf.data_root_folder)
        ut.create_dir_if_not_exists(cf.input_data_sub_folder)
        ut.create_dir_if_not_exists(cf.output_data_sub_folder)

    except PermissionError as err:
        raise err(err.args[0])
        sys.exit(1)

    """Stored the run start time in UTC time to update the watermark table in the database"""
    current_utc_time = datetime.now(timezone.utc)

    """Connects to Gmail and downloads attachments for the new mails since the last run"""
    gmail = Gmail()

    try:
        download_status = download_attachments(gmail=gmail, sender=[sender["sender_email"] for sender in cf.credit_card_sender_config])
    except ConnectionError as err:
        raise(err.args[0])
        sys.exit(1)
    except PermissionError as err:
        raise(err.args[0])
        sys.exit(1)

    try:
        """Checks if any new PDF file has been downloaded. 
        If new files are there, proceed"""
        if download_status[0]:
            """ extract the required part of the newly downloaded PDFs and return it as a list of dicts"""

            pdf_extract_text =\
                read_pdf([(sender, pdf_file_location) for (sender, pdf_file_location) in download_status[1]]\
                            , convert_to_numeric=True)

            """convert the list of dicts to a list of tuples"""
            pdf_extract_text = [(f["bank_name"], f["card_last_4_digits"], f["bill_date"], f["bill_amount"])\
                                for f in pdf_extract_text]

            """Update or insert the data into the SQL database table based on keys"""
            update_or_insert_credit_card_table(pdf_extract=pdf_extract_text)

            last_30_day_bills = ut.read_from_sql(connection=cf.connection, sql_query=cf.sql_get_historic_data.
                                                 format(sql_final_table_name=cf.sql_final_table_name),
                                                 add_header_row=True, args=30)
            if last_30_day_bills:
                export_file_path = cf.output_data_sub_folder +\
                                   "credit_card_last_30_days_" + \
                                   current_utc_time.strftime("%Y-%m-%d_%H%M%S%f") + ".csv"
                ut.write_to_csv(records=last_30_day_bills, filename=export_file_path)

                ut.send_email(gmail=gmail, file_path=export_file_path, subject="Credit card bill for last 30 days")

        else:
            logging.info("No new bills found")

        """Updates the SQL watermark table with current run start time"""
        update_watermark(current_utc_time=current_utc_time)
    except (PdfReadError, ValueError, AttributeError, pyodbc.ProgrammingError, pyodbc.Error) as err:
        raise err[err.args[0]]
        sys.exit(1)
    except Exception as err:
        raise
    finally:
        if cf.connection:
            ut.close_sql_db_connection(connection=cf.connection)















if __name__ == "__main__":
    main()


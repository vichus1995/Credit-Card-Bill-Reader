import logging
import sys

import pyodbc
from simplegmail import Gmail
from simplegmail.query import construct_query
from CreditCardBillExtracter import config as cf, credentials as cr
import utils as ut
import logging_config

logger = logging.getLogger(__name__)


def download_attachments(gmail: Gmail, sender: list[str]) -> int:

    try:
        connection = ut.create_sql_db_connection(server=cr.sql_server_name, database=cr.sql_db_name,
                                                 user=cr.sql_user_name, password=cr.sql_password)
        last_watermark_timestamp = ut.read_from_sql (connection=connection,
                                                    sql_query= cf.sql_get_watermark_timestamp,
                                                args=cf.cred_card_watermark_table_operation_name)[0][0]
        last_watermark_timestamp = last_watermark_timestamp.strftime("%Y/%m/%d")
    except ConnectionError as err:
        raise err(err.args[0])


    query_params = {
        "sender": sender,
        "after": last_watermark_timestamp
    }

    is_new_files_downloaded: bool = False
    try:
        messages = gmail.get_messages(query=construct_query(query_params))
    except PermissionError as e:
        err_message = f"Not enough permissions to perform read from Gmail account: {e}"
        logger.error(err_message)
        raise e(err_message)
    except Exception as e:
        logger.error(e)
        raise

    file_list = []

    for message in messages:
        if message.attachments:

            for attachment in message.attachments:

                out_file_name = attachment.filename.replace('/', '')
                if out_file_name.split(".")[-1].lower() == 'pdf':

                    try:
                        filepath = cf.input_data_sub_folder + out_file_name
                        attachment.save(filepath=filepath, overwrite=True)
                    except PermissionError as err:
                        err_message = f"You don't have required permissions to save {filepath} file to the specified folder"
                        logger.error(err_message)
                        raise err(err_message)

                    file_list.append((message.sender.split("<")[1].split(">")[0], filepath))
                    logger.info(f"{out_file_name} downloaded successfully")

    if file_list:
        is_new_files_downloaded = True

    return is_new_files_downloaded, file_list

import logging

from simplegmail import Gmail
from simplegmail.query import construct_query
from CreditCardBillExtracter import config as cf, credentials as cr
import utils as ut


def download_attachments(gmail: Gmail, sender: list[str]) -> int:

    connection = ut.create_sql_db_connection(server=cr.sql_server_name, database=cr.sql_db_name,
                                             user=cr.sql_user_name, password=cr.sql_password)
    last_watermark_timestamp = ut.read_from_sql (connection=connection,
                                                sql_query= cf.sql_get_watermark_timestamp,
                                            args=cf.cred_card_watermark_table_operation_name)[0][0]
    last_watermark_timestamp = last_watermark_timestamp.strftime("%Y/%m/%d")

    query_params = {
        "sender": sender,
        #"newer_than": (30, "day")
        "after": last_watermark_timestamp
    }

    is_new_files_downloaded: bool = False
    try:
        messages = gmail.get_messages(query=construct_query(query_params))
    except PermissionError as e:
        logging.error(f"Not enough permissions to perform this action: {e}")
    except Exception as e:
        logging.error(e)

    file_list = []

    for message in messages:
        if message.attachments:

            for attachment in message.attachments:
                out_file_name = attachment.filename.replace('/', '')
                filepath = cf.input_data_sub_folder + out_file_name
                attachment.save(filepath=filepath, overwrite=True)
                file_list.append((message.sender.split("<")[1].split(">")[0], filepath))
            is_new_files_downloaded = True

    return is_new_files_downloaded, file_list

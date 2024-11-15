import csv
import os
from CreditCardBillExtracter import config as cf
import pyodbc
import logging


logger = logging.getLogger(__name__)


def create_dir_if_not_exists(directory):
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except PermissionError as err:
        err_message = f"You do not have the required permission to the folder : {directory}"
        logger.error(err_message)
        raise err(err_message)
    except Exception as e:
        logger.error(e)
        raise


def get_bank_config_details(search_key, search_value, return_key):

    for bank in cf.credit_card_sender_config:
        if bank[search_key] == search_value:
            return bank[return_key]

def create_sql_db_connection(server, database, user, password):
    '''
    Take inputs server instance name, database name, username and password
    Return a SQL Server database connection
    '''
    try :
        connection = pyodbc.connect(Driver="{SQL Server}",
                                    Server=server,
                                    Database=database,
                                    UID=user,
                                    PWD=password)

    except pyodbc.Error as e:
        err_message = f"Failed to connect to database with error: SQL STATE: {e.args[0]}, Error: {e.args[1]}"
        logger.critical(err_message, exc_info=True)
        raise ConnectionError(err_message)

    return connection


# Close the database connection
def close_sql_db_connection(connection):
    '''Take input connection and close the database connection'''
    try:
        connection.close()
    except pyodbc.ProgrammingError:
        logger.warning("SQL DB connection could not be closed")


def insert_to_sql(connection, sql_query: str, bulk_insert: bool = False, args=None):

    cursor = connection.cursor()
    try:
        if bulk_insert:
            cursor.executemany(sql_query, args)
        else:
            cursor.execute(sql_query, args)
    except pyodbc.ProgrammingError as e:
        logger.error(f"Query resulted in error: {e} ")
        raise
    except pyodbc.Error as e:
        logger.error(f"Error:{e}")
        raise
    connection.commit()
    cursor.close()


def run_sql_query(connection, sql_query: str):
    cursor = connection.cursor()
    try:
        cursor.execute(sql_query)
    except pyodbc.ProgrammingError as e:
        logger.error(f"Query resulted in error: {e} ")
        raise
    except pyodbc.Error as e:
        logger.error(f"Error:{e}")
        raise
    connection.commit()
    cursor.close()


def read_from_sql(connection, sql_query: str, args=None, add_header_row=False):

    cursor = connection.cursor()
    try:
        return_list = cursor.execute(sql_query, args).fetchall()
    except pyodbc.ProgrammingError as e:
        logger.error(f"Read query resulted in error: {e} ")
        raise
    except pyodbc.Error as e:
        logger.error(f"Error:{e}")
        raise
    if add_header_row:
        columns = [column[0] for column in cursor.description]
        return_list.insert(0, columns)

    connection.commit()
    cursor.close()
    return return_list


def update_sql_query(connection, sql_query: str, args=None):

    cursor = connection.cursor()
    try:
        cursor.execute(sql_query, args)
    except pyodbc.ProgrammingError as e:
        logger.error(f"Could not update tables. Error: {e} ")
        raise
    except pyodbc.Error as e:
        logger.error(f"Error:{e}")
        raise
    connection.commit()
    cursor.close()


def write_to_csv(records, filename):

    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(records)






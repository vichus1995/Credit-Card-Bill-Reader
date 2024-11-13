from CreditCardBillExtracter import config as cf, credentials as cr
import utils as ut
from datetime import datetime

def update_or_insert_credit_card_table(pdf_extract: list[tuple]):

    connection = ut.create_sql_db_connection(server=cr.sql_server_name, database=cr.sql_db_name,
                                user=cr.sql_user_name, password=cr.sql_password)

    ut.run_sql_query(connection=connection, sql_query=cf.sql_truncate_staging_table.format(table_name=cf.sql_staging_table_name))
    ut.insert_to_sql(connection=connection, sql_query=cf.sql_insert_to_staging_table, bulk_insert=True, args=pdf_extract)
    ut.run_sql_query(connection=connection, sql_query=cf.sql_exec_staging_to_final_table_sp)


def update_watermark(current_utc_time:datetime):
    connection = ut.create_sql_db_connection(server=cr.sql_server_name, database=cr.sql_db_name,
                                user=cr.sql_user_name, password=cr.sql_password)

    update_watermark_args = current_utc_time, cf.cred_card_watermark_table_operation_name

    ut.update_sql_query(connection=connection, sql_query=cf.sql_update_watermark_table,\
                     args=update_watermark_args)

    ut.close_sql_db_connection(connection)





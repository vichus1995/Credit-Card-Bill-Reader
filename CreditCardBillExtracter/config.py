import credentials as cr
import utils as ut

#root folder for storing the data
data_root_folder = '../data'
input_data_sub_folder = '../data/input_data/'
output_data_sub_folder = '../data/output_data/'


credit_card_sender_config =\
    [
        {
            "bank_name": "hdfc",
            "bank_name_upper": "HDFC",
            "sender_email": "Emailstatements.cards@hdfcbank.net",
            "password_required": True,
            "pdf_password": cr.credit_card_holder_first_name[:4].upper() + cr.credit_card_holder_dob.strftime("%d%m"),
            "total_amt_due_lookup_text": "Total Dues",
            "bill_date_format": "%d-%m-%Y"
        },
        {
            "bank_name": "icici",
            "bank_name_upper": "ICICI",
            "sender_email": "credit_cards@icicibank.com",
            "password_required": True,
            "pdf_password": cr.credit_card_holder_first_name[:4].lower() + cr.credit_card_holder_dob.strftime("%d%m"),
            "total_amt_due_lookup_text": "Total Amount due",
            "bill_date_format": "%B %d, %Y"
        },
        {
            "bank_name": "sbi",
            "bank_name_upper": "SBI",
            "sender_email": "Statements@sbicard.com",
            "password_required": True,
            "pdf_password": cr.credit_card_holder_dob.strftime("%d%m%Y") + cr.sbi_card_last_4_digits,
            "total_amt_due_lookup_text": "Total Amount Due",
            "bill_date_format": "%d%m%Y"
        }
    ]

#SQL queries

sql_staging_table_name = '''dbo.Staging_CreditCardInfo'''
sql_final_table_name = '''dbo.CreditCardInfo'''
watermark_table_name = '''dbo.Operation_Watermark'''
sql_staging_to_final_table_sp_name = '''dbo.usp_Update_CreditCardInfo_from_Staging'''
cred_card_watermark_table_operation_name = '''CreditCardReader'''

sql_truncate_staging_table = '''TRUNCATE TABLE {table_name}'''

sql_insert_to_staging_table = '''INSERT INTO {staging_table_name}(BankName, CardLast4Digits, BillDate, TotalAmountDue) 
                                VALUES(?, ?, ?, ?)'''.format(staging_table_name=sql_staging_table_name)

sql_exec_staging_to_final_table_sp = '''EXEC {sp_name}'''.format(sp_name=sql_staging_to_final_table_sp_name)

sql_update_watermark_table = f'''UPDATE {watermark_table_name} SET LastUpdatedTimestamp = ? WHERE OperationName = ?'''
sql_get_watermark_timestamp = f'''SELECT LastUpdatedTimestamp FROM {watermark_table_name} WHERE OperationName = ?'''

sql_get_historic_data = '''SELECT BankName, CardLast4Digits, BillDate, TotalAmountDue FROM {sql_final_table_name}\
                        WHERE BillDate > GETUTCDATE() - ?'''

connection = ut.create_sql_db_connection(server=cr.sql_server_name, database=cr.sql_db_name,
                                         user=cr.sql_user_name, password=cr.sql_password)












import datetime

from CreditCardBillExtracter import config as cf
import utils as ut
from simplegmail import Gmail
from read_emails import download_attachments
from read_pdf import read_pdf
from sql_crud import update_or_insert_credit_card_table, update_watermark
from datetime import datetime, timezone
import logging


logger = logging.getLogger(__name__)


def main():
    ut.create_dir_if_not_exists(cf.data_root_folder)
    ut.create_dir_if_not_exists(cf.input_data_sub_folder)
    ut.create_dir_if_not_exists(cf.output_data_sub_folder)
    current_utc_time = datetime.now(timezone.utc)

    gmail = Gmail()
    download_status = download_attachments(gmail=gmail, sender=[sender["sender_email"] for sender in cf.credit_card_sender_config])

    if download_status[0]:
        pdf_extract_text =\
            read_pdf([(sender, pdf_file_location) for (sender, pdf_file_location) in download_status[1]]\
                        , convert_to_numeric=True)

        pdf_extract_text = [(f["bank_name"], f["card_last_4_digits"], f["bill_date"], f["bill_amount"])\
                            for f in pdf_extract_text]

        update_or_insert_credit_card_table(pdf_extract=pdf_extract_text)

    else:
        logging.info("No new bills found")

    update_watermark(current_utc_time=current_utc_time)















if __name__ == "__main__":
    main()


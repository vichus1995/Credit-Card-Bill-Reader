import utils as ut
from pypdf import PdfReader
from pypdf.errors import PdfReadError
from CreditCardBillExtracter import credentials as cr
from datetime import datetime as dt
import logging

logger = logging.getLogger(__name__)


def read_pdf(pdf_file_list: list[str], convert_to_numeric: bool):
    print(pdf_file_list)
    pdf_text_extract = []
    span_text = None
    for sender, pdf_file in pdf_file_list:
        print(pdf_file)
        if pdf_file.split(".")[-1].lower() != 'pdf':
            continue

        try:
            reader = PdfReader(pdf_file)
            if ut.get_bank_config_details(search_key="sender_email", search_value=sender, return_key="password_required"):
                reader.decrypt(ut.get_bank_config_details\
                                   (search_key="sender_email", search_value=sender, return_key="pdf_password"))

            page_content = reader.pages[0].extract_text().split("\n")
        except PdfReadError as e:
            logger.error(f"Unable to read PDF file: {e}")
        except Exception as e:
            logger.error(e)

        try:
            lookup_text = ut.get_bank_config_details\
                                   (
                                    search_key="sender_email",\
                                    search_value=sender,\
                                    return_key="total_amt_due_lookup_text"
                )

            bank_name = ut.get_bank_config_details\
                (search_key="sender_email", search_value=sender, return_key="bank_name")
            bill_date_format = ut.get_bank_config_details\
                (search_key="sender_email", search_value=sender, return_key="bill_date_format")
            for line, text in enumerate(page_content):
                if lookup_text in text:
                    if bank_name == "icici":
                        span_text = page_content[line+11][1:]
                        card_last_4_digits = pdf_file.split("/")[-1].split("_")[0][-4:]
                        bill_date = dt.strptime(page_content[36], bill_date_format)
                    elif bank_name == 'hdfc':
                        span_text = page_content[line+1].split()[1]
                        card_last_4_digits = page_content[13][-5:-1]
                        bill_date = dt.strptime(pdf_file.split("/")[-1].split("_")[1].split(".")[0], bill_date_format)
                    elif bank_name == 'sbi':
                        span_text = page_content[line+60]
                        card_last_4_digits = cr.sbi_card_last_4_digits
                        bill_date = dt.strptime(pdf_file.split("/")[-1].split("_")[1].split(".")[0], bill_date_format)
                    break

            if convert_to_numeric:
                span_text_value = float(span_text.replace(",",""))
                pdf_text_extract.append((bank_name, card_last_4_digits, bill_date.strftime("%Y-%m-%d"), span_text_value))
            else:
                pdf_text_extract.append(span_text)

        except AttributeError as e:
            logger.error(f"No matching text found in {pdf_file}")

        except ValueError as e:
            logger.error(f"Extracted value is not numeric in {pdf_file}")
        except Exception as e:
            logger.error(e)

    return [{"bank_name": item[0],\
             "card_last_4_digits": item[1],\
             "bill_date": item[2],\
             "bill_amount": item[3]} for item in pdf_text_extract]

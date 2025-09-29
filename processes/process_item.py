"""Module to handle item processing"""

import logging

import pandas as pd

from dotenv import load_dotenv

from io import BytesIO

from mbu_dev_shared_components.database.connection import RPAConnection

from mbu_msoffice_integration.sharepoint_class import Sharepoint

from helpers import helper_functions
from helpers.config import WEBFORMS_CONFIG

load_dotenv()  # Loads variables from .env

SHAREPOINT_SITE_URL = "https://aarhuskommune.sharepoint.com"
SHAREPOINT_DOCUMENT_LIBRARY = "Delte dokumenter"

SHEET_NAME = "Besvarelser"

RPA_CONN = RPAConnection(db_env="PROD", commit=False)
with RPA_CONN:
    OS2_API_KEY = RPA_CONN.get_credential("os2_api").get("decrypted_password", "")

logger = logging.getLogger(__name__)


def process_item(item_data: dict, sharepoint_kwargs: dict):
    """Function to handle item processing"""

    config = item_data.get("config", {})

    site_name = config["site_name"]
    folder_name = config["folder_name"]
    excel_file_name = config["excel_file_name"]
    excel_file_exists = config.get("excel_file_exists", False)

    formular_mapping = WEBFORMS_CONFIG["henvisningsskema_til_klinisk_hyp"]["formular_mapping"]

    upload_pdfs_to_sharepoint_folder_name = config.get("upload_pdfs_to_sharepoint_folder_name", "")
    file_url = config.get("file_url", "")

    new_submissions = item_data.get("submissions", [])

    try:
        sharepoint_api = Sharepoint(
            tenant=sharepoint_kwargs["tenant"],
            client_id=sharepoint_kwargs["client_id"],
            thumbprint=sharepoint_kwargs["thumbprint"],
            cert_path=sharepoint_kwargs["cert_path"],
            site_url=SHAREPOINT_SITE_URL,
            site_name=site_name,
            document_library=SHAREPOINT_DOCUMENT_LIBRARY,
        )

    except Exception as e:
        logger.info(f"Error when trying to authenticate: {e}")

    # If the Excel file does not exist, we create it with all existing submissions
    if not excel_file_exists:
        logger.info(f"Excel file '{excel_file_name}' not found - creating new")

        # Force column order according to formular_mapping
        column_order = list(formular_mapping.values())

        normalized_submissions = [
            {col: row.get(col, None) for col in column_order}
            for row in new_submissions
        ]

        all_submissions_df = pd.DataFrame(normalized_submissions, columns=column_order)

        # Ensure no extra columns slipped in
        all_submissions_df = all_submissions_df[column_order]

        excel_stream = BytesIO()
        all_submissions_df.to_excel(
            excel_stream,
            index=False,
            engine="openpyxl",
            sheet_name=SHEET_NAME
        )
        excel_stream.seek(0)

        try:
            sharepoint_api.upload_file_from_bytes(
                binary_content=excel_stream.getvalue(),
                file_name=excel_file_name,
                folder_name=folder_name,
            )

        except Exception as e:
            logger.info(f"Error when trying to upload excel file to SharePoint: {e}")

    elif excel_file_exists:
        logger.info(f"Excel file '{excel_file_name}' already exists - appending new rows")

        try:
            sharepoint_api.append_row_to_sharepoint_excel(
                folder_name=folder_name,
                excel_file_name=excel_file_name,
                sheet_name=SHEET_NAME,
                new_rows=new_submissions,
            )

        except Exception as e:
            logger.info(f"Error when trying to append row to existing excel file in SharePoint: {e}")

    logger.info("Formatting and sorting excel file")
    try:
        sharepoint_api.format_and_sort_excel_file(
            folder_name=folder_name,
            excel_file_name=excel_file_name,
            sheet_name=SHEET_NAME,
            sorting_keys=[{"key": "A", "ascending": False, "type": "int"}],
            bold_rows=[1],
            align_horizontal="left",
            align_vertical="top",
            italic_rows=None,
            font_config=None,
            column_widths=100,
            freeze_panes="A2",
        )

    except Exception as e:
        logger.info(f"Error when trying format and sort excel file: {e}")

    if upload_pdfs_to_sharepoint_folder_name != "":
        logger.info("Uploading PDFs to SharePoint")

        helper_functions.upload_pdf_to_sharepoint(
            sharepoint_api=sharepoint_api,
            folder_name=upload_pdfs_to_sharepoint_folder_name,
            os2_api_key=OS2_API_KEY,
            file_url=file_url,
        )

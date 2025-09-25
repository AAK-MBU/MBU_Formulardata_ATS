"""Module to handle item processing"""

from dotenv import load_dotenv

from io import BytesIO

from mbu_dev_shared_components.msoffice365.sharepoint_api.files import Sharepoint

from helpers import helper_functions
from helpers import formular_mappings

load_dotenv()  # Loads variables from .env

SHEET_NAME = "Besvarelser"


def process_item(item_data: dict, sharepoint_api: Sharepoint, os2_api_key: str):
    """Function to handle item processing"""
    print(item_data)

    os2_webform_id = item_data.get("os2_webform_id", "")
    config = item_data.get("config", {})
    new_submissions = item_data.get("submissions", [])

    site_name = config["site_name"]
    folder_name = config["folder_name"]
    excel_file_name = config["excel_file_name"]
    formular_mapping = config["formular_mapping"]

    excel_file_exists = config.get("excel_file_exists", False)

    print("STEP 2 - Checking if Excel file already exists in Sharepoint folder.")
    # If the Excel file does not exist, we create it with all existing submissions
    if not excel_file_exists:
        print(f"Excel file '{config['excel_file_name']}' not found - creating new.")

        all_submissions_df = helper_functions.build_df(new_submissions, config["formular_mapping"])

        excel_stream = BytesIO()
        all_submissions_df.to_excel(excel_stream, index=False, engine="openpyxl", sheet_name=SHEET_NAME)
        excel_stream.seek(0)

        sharepoint_api.upload_file_from_bytes(
            binary_content=excel_stream.getvalue(),
            file_name=excel_file_name,
            folder_name=folder_name
        )

        sharepoint_api.format_and_sort_excel_file(
            folder_name=folder_name,
            excel_file_name=excel_file_name,
            sheet_name=SHEET_NAME,
            sorting_keys=[{"key": "A", "ascending": False, "type": "str"}],
            bold_rows=[1],
            align_horizontal="left",
            align_vertical="top",
            italic_rows=None,
            font_config=None,
            column_widths=100,
            freeze_panes="A2"
        )

        return
    


    return

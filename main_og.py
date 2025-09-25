"""
main.py
"""

import sys
import os

import asyncio

import datetime

from io import BytesIO

import pandas as pd

from dotenv import load_dotenv

from automation_server_client import AutomationServer, Workqueue, WorkItemError

from mbu_dev_shared_components.msoffice365.sharepoint_api.files import Sharepoint

from mbu_dev_shared_components.database.connection import RPAConnection

from helpers import helper_functions
from helpers import formular_mappings

# from sub_processes import populate_queue_flow
# from sub_processes import process_queue_flow

load_dotenv()  # Loads variables from .env

ATS_URL = os.getenv("ATS_URL")
ATS_TOKEN = os.getenv("ATS_TOKEN")

DB_CONN_STRING = os.getenv("DBConnectionStringProd")
# DB_CONN_STRING = os.getenv("DbConnectionString")  # UNCOMMENT FOR DEV TESTING

# TEMPORARY OVERRIDE: Set a new env variable in memory only
os.environ["DbConnectionString"] = os.getenv("DBConnectionStringProd")

RPA_CONN = RPAConnection(db_env="PROD", commit=False)
with RPA_CONN:
    SCV_LOGIN = RPA_CONN.get_credential("SvcRpaMbu002")
    USERNAME = SCV_LOGIN.get("username", "")
    PASSWORD = SCV_LOGIN.get("decrypted_password", "")

    OS2_API_KEY = RPA_CONN.get_credential("os2_api").get("decrypted_password", "")

SHAREPOINT_FOLDER_URL = "https://aarhuskommune.sharepoint.com"
SHAREPOINT_DOCUMENT_LIBRARY = "Delte dokumenter"

SHEET_NAME = "Besvarelser"


async def populate_queue(workqueue: Workqueue):
    """
    Function to populate the workqueue with items.
    """

    print("Hello from populate workqueue!\n")

    today = datetime.date.today()
    # today = datetime.date(2025, 5, 26)
    monday_last_week = today - datetime.timedelta(days=today.weekday() + 7)
    sunday_last_week = monday_last_week + datetime.timedelta(days=6)

    webforms_config = {
        "basisteam_spoergeskema_til_fagpe": {
            "site_name": "tea-teamsite8906",
            "folder_name": "General/Evaluering/Udtræk OS2Forms",
            "formular_mapping": formular_mappings.basisteam_spoergeskema_til_fagpe_mapping,
            "excel_file_name": "Dataudtræk basisteam - fagperson.xlsx",
            "upload_pdfs_to_sharepoint_folder_name": "General/Evaluering/Besvarelser fra OS2Forms - fagpersoner",
        },
        "basisteam_spoergeskema_til_forae": {
            "site_name": "tea-teamsite8906",
            "folder_name": "General/Evaluering/Udtræk OS2Forms",
            "formular_mapping": formular_mappings.basisteam_spoergeskema_til_forae_mapping,
            "excel_file_name": "Dataudtræk basisteam - forældre.xlsx",
            "upload_pdfs_to_sharepoint_folder_name": "General/Evaluering/Besvarelser fra OS2Forms - forældre",
        },
        "henvisningsskema_til_klinisk_hyp": {
            "site_name": "tea-teamsite10693",
            "folder_name": "General/Udtræk OS2Forms/Henvisningsskema",
            "formular_mapping": formular_mappings.henvisningsskema_til_klinisk_hyp_mapping,
            "excel_file_name": "Dataudtræk henvisningsskema hypnoterapi.xlsx",
        },
        "spoergeskema_hypnoterapi_foer_fo": {
            "site_name": "tea-teamsite10693",
            "folder_name": "General/Udtræk OS2Forms/Spørgeskema",
            "formular_mapping": formular_mappings.spoergeskema_hypnoterapi_foer_fo_mapping,
            "excel_file_name": "Dataudtræk spørgeskema hypnoterapi.xlsx",
        },
        "opfoelgende_spoergeskema_hypnote": {
            "site_name": "tea-teamsite10693",
            "folder_name": "General/Udtræk OS2Forms/Opfølgende spørgeskema",
            "formular_mapping": formular_mappings.opfoelgende_spoergeskema_hypnote_mapping,
            "excel_file_name": "Dataudtræk opfølgende spørgeskema hypnoterapi.xlsx",
        },
        "foraelder_en_god_overgang_fra_hj": {
            "site_name": "tea-teamsite10533",
            "folder_name": "General/Udtræk data OS2Forms/Opfølgende spørgeskema forældre",
            "formular_mapping": formular_mappings.foraelder_en_god_overgang_fra_hj_mapping,
            "excel_file_name": "Dataudtræk en god overgang fra hjem til dagtilbud - forælder.xlsx",
        },
        "fagperson_en_god_overgang_fra_hj": {
            "site_name": "tea-teamsite10533",
            "folder_name": "General/Udtræk data OS2Forms/Opfølgende spørgeskema fagpersonale",
            "formular_mapping": formular_mappings.fagperson_en_god_overgang_fra_hj_mapping,
            "excel_file_name": "Dataudtræk en god overgang fra hjem til dagtilbud - fagperson.xlsx",
        },
        "sundung_aarhus": {
            "site_name": "tea-teamsite11121",
            "folder_name": "General/Udtræk OS2-formularer",
            "formular_mapping": formular_mappings.sundung_aarhus_mapping,
            "excel_file_name": "Dataudtræk SundUng Aarhus.xlsx",
        },
        "tilmelding_til_modersmaalsunderv": {
            "site_name": "Teams-Modersmlsundervisning",
            "folder_name": "General",
            "formular_mapping": formular_mappings.tilmelding_til_modersmaalsunderv_mapping,
            "excel_file_name": f"Dataudtræk - {monday_last_week} til {sunday_last_week}.xlsx",
        },
    }

    upload_pdfs_to_sharepoint_folder_name = ""

    existing_workqueue_items = helper_functions.get_workqueue_items(
        url=ATS_URL, token=ATS_TOKEN, workqueue_id=workqueue.id
    )

    for os2_webform_id, config in webforms_config.items():
        if os2_webform_id in existing_workqueue_items:
            continue

        if not config:
            continue

        if os2_webform_id not in ("spoergeskema_hypnoterapi_foer_fo",):
            continue

        new_submissions = []

        site_name = config["site_name"]
        folder_name = config["folder_name"]
        excel_file_name = config["excel_file_name"]
        formular_mapping = config["formular_mapping"]

        upload_pdfs_to_sharepoint_folder_name = config.get(
            "upload_pdfs_to_sharepoint_folder_name", ""
        )

        testing = True
        if testing:
            site_name = "MBURPA"
            folder_name = "Automation_Server"
            upload_pdfs_to_sharepoint_folder_name = "Automation_Server/pdf"

        sharepoint_api = Sharepoint(
            username=USERNAME,
            password=PASSWORD,
            site_url=SHAREPOINT_FOLDER_URL,
            site_name=site_name,
            document_library=SHAREPOINT_DOCUMENT_LIBRARY,
        )

        print(f"Looping through submissions for webform_id: {os2_webform_id}")

        print("STEP 1 - Fetching all active submissions.\n")
        all_submissions = helper_functions.get_forms_data(
            conn_string=DB_CONN_STRING,
            form_type=os2_webform_id,
        )
        print(
            f"OS2 submissions retrieved. {len(all_submissions)} total submissions found."
        )

        # Modersmaalsundervisning has a different flow - therefore we skip the Excel overwrite functionality if we are currently running that formular
        if os2_webform_id != "tilmelding_til_modersmaalsunderv":
            # We start by fetching the list of existing Excel files in Sharepoint
            files_in_sharepoint = sharepoint_api.fetch_files_list(
                folder_name=folder_name
            )
            file_names = [f["Name"] for f in files_in_sharepoint]

            print(
                "STEP 2 - Checking if Excel file already exists in Sharepoint folder."
            )
            # If the Excel file does not exist, we create it with all existing submissions
            if excel_file_name not in file_names:
                print(f"Excel file '{excel_file_name}' not found - creating new.")

                all_submissions_df = helper_functions.build_df(
                    all_submissions, formular_mapping
                )

                excel_stream = BytesIO()
                all_submissions_df.to_excel(
                    excel_stream, index=False, engine="openpyxl", sheet_name=SHEET_NAME
                )
                excel_stream.seek(0)

                sharepoint_api.upload_file_from_bytes(
                    binary_content=excel_stream.getvalue(),
                    file_name=excel_file_name,
                    folder_name=folder_name,
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
                    freeze_panes="A2",
                )

                # We continue to the next form as there is no need to add existing submissions to the workqueue, as they are already in the Excel file
                continue

            # If the Excel file does exist, we fetch it and load it into a DataFrame, so we can compare serial numbers
            print("STEP 3 - Retrieving existing Excel sheet.")
            excel_file = sharepoint_api.fetch_file_using_open_binary(
                excel_file_name, folder_name
            )
            excel_stream = BytesIO(excel_file)
            excel_file_df = pd.read_excel(io=excel_stream, sheet_name=SHEET_NAME)

            # Create a set of serial numbers from the Excel file
            serial_set = set(excel_file_df["Serial number"].tolist())
            print(
                f"Excel file retrieved. {len(excel_file_df)} rows found in existing sheet."
            )

            # Loop through all active submissions and transform them to the correct format
            print(
                "STEP 4 - Looping submissions and mapping retrieved data to fit Excel column names."
            )
            for form in all_submissions:
                form_serial_number = form["entity"]["serial"][0]["value"]

                # If the form's serial number is already in the Excel file, skip it
                if form_serial_number in serial_set:
                    continue

                transformed_row = helper_functions.transform_form_submission(
                    form_serial_number, form, formular_mapping
                )

                new_submissions.append(transformed_row)

                work_item_data = {
                    "site_name": site_name,
                    "folder_name": folder_name,
                    "excel_file_name": excel_file_name,
                    "data": new_submissions,
                }

                if (
                    "upload_pdfs_to_sharepoint_folder_name" in config
                    and upload_pdfs_to_sharepoint_folder_name != ""
                ):
                    work_item_data["upload_pdfs_to_sharepoint_folder_name"] = (
                        upload_pdfs_to_sharepoint_folder_name
                    )
                    work_item_data["pdf_url"] = form["data"]["attachments"][
                        "besvarelse_i_pdf_format"
                    ]["url"]

                print("STEP 5 - Adding new submission to workqueue.")
                workqueue.add_item(reference=os2_webform_id, data=work_item_data)

                print(
                    f"Added submissions for webform, {os2_webform_id}, to workqueue.\n"
                )


async def process_workqueue(workqueue: Workqueue):
    """
    Function to process the workqueue items.
    """

    print("Hello from process workqueue!")

    for item in workqueue:
        reference = item.reference

        data = item.data

        site_name = data.get("site_name")
        folder_name = data.get("folder_name")
        excel_file_name = data.get("excel_file_name")
        form_data = data.get("data")

        upload_pdfs_to_sharepoint_folder_name = data.get(
            "upload_pdfs_to_sharepoint_folder_name", ""
        )

        file_url = data.get("pdf_url", "")

        testing = True
        # testing = False
        if testing:
            site_name = "MBURPA"
            folder_name = "Automation_Server"
            upload_pdfs_to_sharepoint_folder_name = "Automation_Server/pdf"

        sharepoint_api = Sharepoint(
            username=USERNAME,
            password=PASSWORD,
            site_url=SHAREPOINT_FOLDER_URL,
            site_name=site_name,
            document_library=SHAREPOINT_DOCUMENT_LIBRARY,
        )

        with item:
            try:
                # Process the item here
                print(f"Processing item with reference: {reference}")

                sharepoint_api.append_row_to_sharepoint_excel(
                    folder_name=folder_name,
                    excel_file_name=excel_file_name,
                    sheet_name=SHEET_NAME,
                    new_rows=form_data,
                )

                if upload_pdfs_to_sharepoint_folder_name != "":
                    print("Uploading PDFs to SharePoint.")

                    helper_functions.upload_pdf_to_sharepoint(
                        sharepoint_api=sharepoint_api,
                        folder_name=upload_pdfs_to_sharepoint_folder_name,
                        os2_api_key=OS2_API_KEY,
                        file_url=file_url,
                    )

            except WorkItemError as e:
                # A WorkItemError represents a soft error that indicates the item should be passed to manual processing or a business logic fault
                print(f"Error processing item: {data}. Error: {e}")

                item.fail(str(e))

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
            freeze_panes="A2",
        )

        print()


if __name__ == "__main__":
    ats = AutomationServer.from_environment()

    test_workqueue = ats.workqueue()

    print(f"Workqueue: {test_workqueue}\n")

    if "--queue" in sys.argv:
        asyncio.run(populate_queue(test_workqueue))

        print("Workqueue populated with new items.\n")

        sys.exit()

    else:
        asyncio.run(process_workqueue(test_workqueue))

        print("Workqueue processing completed.")

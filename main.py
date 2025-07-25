"""
main.py
"""

import sys
import os
import json

import asyncio

import datetime

import pandas as pd

from io import BytesIO

from dotenv import load_dotenv

from automation_server_client import AutomationServer, Workqueue, WorkItemError

from mbu_dev_shared_components.msoffice365.sharepoint_api.files import Sharepoint

from helper_functions import helper_functions

from helper_functions import formular_mappings


LINE_BREAK = "\n\n\n" + "-" * 125 + "\n\n\n"

load_dotenv()  # Loads variables from .env

USERNAME = os.getenv("SvcRpaMBU002_USERNAME")  # Change to fetch from automation server credential
PASSWORD = os.getenv("SvcRpaMBU002_PASSWORD")  # Change to fetch from automation server credential

DB_CONN_STRING = os.getenv("DbConnectionString")  # Change to fetch from automation server credential?

ATS_URL = os.getenv("ATS_URL")
ATS_TOKEN = os.getenv("ATS_TOKEN")

SHAREPOINT_FOLDER_URL = "https://aarhuskommune.sharepoint.com"
SHAREPOINT_DOCUMENT_LIBRARY = "Delte dokumenter"

SHEET_NAME = "Besvarelser"


async def populate_queue(workqueue: Workqueue):
    """
    Function to populate the workqueue with items.
    """

    print("Hello from populate workqueue!\n")

    site_name = ""
    folder_name = ""
    excel_file_name = ""
    formular_mapping = None

    upload_pdfs_to_sharepoint_folder_name = ""

    pdf_url = ""

    current_day_of_month = str(pd.Timestamp.now().day)

    print(f"Current day of month: {current_day_of_month}\nCurrent day of month value type: {type(current_day_of_month)}\n")

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

    existing_workqueue_items = helper_functions.get_workqueue_items(
        url=ATS_URL,
        token=ATS_TOKEN,
        workqueue_id=workqueue.id
    )

    for os2_webform_id, config in webforms_config.items():
        if os2_webform_id not in (
            "basisteam_spoergeskema_til_fagpe",
            # "henvisningsskema_til_klinisk_hyp",
            # "spoergeskema_hypnoterapi_foer_fo",
            # "spoergeskema_hypnoterapi_foer_fo",
        ):
            continue

        config = webforms_config.get(os2_webform_id)
        if not config:
            continue

        site_name = config["site_name"]
        folder_name = config["folder_name"]
        excel_file_name = config["excel_file_name"]
        formular_mapping = config["formular_mapping"]

        if os2_webform_id in ("basisteam_spoergeskema_til_fagpe", "basisteam_spoergeskema_til_forae"):
            upload_pdfs_to_sharepoint_folder_name = config.get("upload_pdfs_to_sharepoint_folder_name", "")

        testing = True
        # testing = False
        if testing:
            site_name = "MBURPA"
            folder_name = "Automation_Server"
            upload_pdfs_to_sharepoint_folder_name = "Automation_Server/pdf"

        sharepoint_api = Sharepoint(username=USERNAME, password=PASSWORD, site_url=SHAREPOINT_FOLDER_URL, site_name=site_name, document_library=SHAREPOINT_DOCUMENT_LIBRARY)

        print(f"Looping through submissions for webform_id: {os2_webform_id}\n")

        new_forms = 0
        new_forms__already_in_workqueue = 0

        print("STEP 1 - Fetching all active forms.\n")
        all_forms = helper_functions.get_forms_data(
            conn_string=DB_CONN_STRING,
            form_type=os2_webform_id,
        )
        print(f"OS2 forms retrieved. {len(all_forms)} active forms found.\n")

        # Modersmaalsundervisning has a different flow - therefore we skip the Excel overwrite functionality if we are currently running that formular
        if os2_webform_id != "tilmelding_til_modersmaalsunderv":
            # STEP 2 - Get the Excel file from Sharepoint
            print("STEP 2 - Retrieving existing Excel sheet.\n")
            excel_file = sharepoint_api.fetch_file_using_open_binary(excel_file_name, folder_name)
            excel_stream = BytesIO(excel_file)
            excel_file_df = pd.read_excel(io=excel_stream, sheet_name=SHEET_NAME)
            print(f"Excel file retrieved. {len(excel_file_df)} rows found in existing sheet.\n")

            # Create a set of serial numbers from the Excel file
            serial_set = set(excel_file_df["Serial number"].tolist())

            # STEP 3 - Loop through all active forms and transform them to the correct format
            print("STEP 3 - Looping forms and mapping retrieved data to fit Excel column names.\n")
            for form in all_forms:
                form_serial_number = form["entity"]["serial"][0]["value"]

                form_uuid = form["entity"]["uuid"][0]["value"]

                if form_serial_number in serial_set:
                    continue

                new_forms += 1

                if form_uuid in existing_workqueue_items:
                    print(f"Form with UUID {form_uuid} already exists in workqueue, skipping.\n")

                    new_forms__already_in_workqueue += 1

                    continue

                transformed_row = formular_mappings.transform_form_submission(form_serial_number, form, formular_mapping)

                work_item_data = {
                    "webform_id": os2_webform_id,
                    "site_name": site_name,
                    "folder_name": folder_name,
                    "excel_file_name": excel_file_name,
                    "data": transformed_row,
                }

                if os2_webform_id in ("basisteam_spoergeskema_til_fagpe", "basisteam_spoergeskema_til_forae"):
                    work_item_data["upload_pdfs_to_sharepoint_folder_name"] = upload_pdfs_to_sharepoint_folder_name

                    work_item_data["pdf_url"] = form["data"]["attachments"]["besvarelse_i_pdf_format"]["url"]

                workqueue.add_item(
                    data=work_item_data,
                    reference=form_uuid
                )

                print(f"Added form with reference: {form_uuid} to workqueue.\n")

            print(f"New forms not in Excel file: {new_forms}. Of these, {new_forms__already_in_workqueue} are already present in workqueue\n")

        print(LINE_BREAK)


async def process_workqueue(workqueue: Workqueue):
    """
    Function to process the workqueue items.
    """

    print("Hello from process workqueue!")

    os2_api_key = os.getenv("OS2_API_KEY")  # Change to fetch from automation server credential

    for item in workqueue:
        reference = item.reference

        data = item.get_data_as_dict()

        webform_id = data.get("webform_id")
        site_name = data.get("site_name")
        folder_name = data.get("folder_name")
        excel_file_name = data.get("excel_file_name")
        form_data = data.get("data")

        upload_pdfs_to_sharepoint_folder_name = ""

        file_url = ""

        testing = True
        # testing = False
        if testing:
            site_name = "MBURPA"
            folder_name = "Automation_Server"
            upload_pdfs_to_sharepoint_folder_name = "Automation_Server/pdf"

        if webform_id in ("basisteam_spoergeskema_til_fagpe", "basisteam_spoergeskema_til_forae"):
            upload_pdfs_to_sharepoint_folder_name = data.get("upload_pdfs_to_sharepoint_folder_name", "")

            file_url = data.get("pdf_url", "")

        sharepoint_api = Sharepoint(username=USERNAME, password=PASSWORD, site_url=SHAREPOINT_FOLDER_URL, site_name=site_name, document_library=SHAREPOINT_DOCUMENT_LIBRARY)

        with item:
            try:
                # Process the item here
                print(f"Processing item with reference: {reference}")

                sharepoint_api.append_row_to_sharepoint_excel(
                    required_headers=["Serial number"],
                    folder_name=folder_name,
                    excel_file_name=excel_file_name,
                    sheet_name=SHEET_NAME,
                    new_row=form_data
                )

                sharepoint_api.format_and_sort_excel_file(
                    folder_name=folder_name,
                    excel_file_name=excel_file_name,
                    sheet_name=SHEET_NAME,
                    sorting_keys=[{"key": "A", "ascending": True, "type": "str"}],
                    bold_rows=[1],
                    align_horizontal="left",
                    align_vertical="up",
                    freeze_panes="1"
                )

                if upload_pdfs_to_sharepoint_folder_name != "":
                    print("Uploading PDFs to SharePoint.")

                    helper_functions.upload_pdf_to_sharepoint(
                        sharepoint_api=sharepoint_api,
                        folder_name=upload_pdfs_to_sharepoint_folder_name,
                        os2_api_key=os2_api_key,
                        file_url=file_url,
                    )

            except WorkItemError as e:
                # A WorkItemError represents a soft error that indicates the item should be passed to manual processing or a business logic fault
                print(f"Error processing item: {data}. Error: {e}")

                item.fail(str(e))

        print()

if __name__ == "__main__":
    ats = AutomationServer.from_environment()

    test_workqueue = ats.workqueue()

    print("Workqueue:", test_workqueue)

    print("TEST PRINT")

    print(sys.argv)

    if "--queue" in sys.argv:
        print("Queue argument detected, clearing workqueue...")

        # test_workqueue.clear_workqueue("new")

        asyncio.run(populate_queue(test_workqueue))

        print("Workqueue populated with new items.")

        sys.exit()

    asyncio.run(process_workqueue(test_workqueue))
    print("Workqueue processing completed.")

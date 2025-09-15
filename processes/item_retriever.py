"""Module to hande queue population"""

import sys
import os

import datetime

from io import BytesIO

import pandas as pd

from dotenv import load_dotenv

from automation_server_client import Workqueue

from mbu_dev_shared_components.msoffice365.sharepoint_api.files import Sharepoint

from processes import helper_functions
from processes import formular_mappings

load_dotenv()  # Loads variables from .env

ATS_URL = os.getenv("ATS_URL")
ATS_TOKEN = os.getenv("ATS_TOKEN")

# DB_CONN_STRING = os.getenv("DBConnectionStringProd")
DB_CONN_STRING = os.getenv("DbConnectionString")  # UNCOMMENT FOR DEV TESTING

SHEET_NAME = "Besvarelser"


def item_retriever(workqueue: Workqueue, sharepoint_api: Sharepoint) -> list[dict]:
    """
    Function to populate the workqueue with items.
    """

    print("Hello from populate workqueue!\n")

    ### STUFF FOR MODERSMAALSUNDERVISNING ###
    # today = datetime.date.today()
    # # today = datetime.date(2025, 5, 26)
    # monday_last_week = today - datetime.timedelta(days=today.weekday() + 7)
    # sunday_last_week = monday_last_week + datetime.timedelta(days=6)
    ### STUFF FOR MODERSMAALSUNDERVISNING ###

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
        # "tilmelding_til_modersmaalsunderv": {
        #     "site_name": "Teams-Modersmlsundervisning",
        #     "folder_name": "General",
        #     "formular_mapping": formular_mappings.tilmelding_til_modersmaalsunderv_mapping,
        #     "excel_file_name": f"Dataudtræk - {monday_last_week} til {sunday_last_week}.xlsx",
        # },
    }

    existing_workqueue_items = helper_functions.get_workqueue_items(
        url=ATS_URL,
        token=ATS_TOKEN,
        workqueue_id=workqueue.id
    )

    queue_items = []

    for os2_webform_id, config in webforms_config.items():
        if os2_webform_id in existing_workqueue_items:
            continue

        if not config:
            continue

        if os2_webform_id not in (
            "spoergeskema_hypnoterapi_foer_fo",
        ):
            continue

        site_name = config["site_name"]
        folder_name = config["folder_name"]
        excel_file_name = config["excel_file_name"]
        formular_mapping = config["formular_mapping"]

        upload_pdfs_to_sharepoint_folder_name = config.get("upload_pdfs_to_sharepoint_folder_name", "")

        config["excel_file_exists"] = False

        ### FOR DEV TESTING ONLY - OVERRIDE SITE AND FOLDER NAME TO AVOID POLLUTING ACTUAL FOLDERS ###
        testing = True
        if testing:
            site_name = "MBURPA"
            folder_name = "Automation_Server"
            if "upload_pdfs_to_sharepoint_folder_name" in config:
                upload_pdfs_to_sharepoint_folder_name = "Automation_Server/pdf"
        ### FOR DEV TESTING ONLY - OVERRIDE SITE AND FOLDER NAME TO AVOID POLLUTING ACTUAL FOLDERS ###

        sharepoint_api.site_name = site_name

        print(f"Looping through submissions for webform_id: {os2_webform_id}")

        print("STEP 1 - Fetching all active submissions.\n")
        all_submissions = helper_functions.get_forms_data(
            conn_string=DB_CONN_STRING,
            form_type=os2_webform_id,
        )
        print(f"OS2 submissions retrieved. {len(all_submissions)} total submissions found.")

        new_submissions = []

        serial_set = set()

        files_in_sharepoint = sharepoint_api.fetch_files_list(folder_name=folder_name)
        file_names = [f["Name"] for f in files_in_sharepoint]

        if excel_file_name in file_names:
            config["excel_file_exists"] = True

            # If the Excel file exists, we fetch it and load it into a DataFrame, so we can compare serial numbers
            print("STEP 3 - Retrieving existing Excel sheet.")
            excel_file = sharepoint_api.fetch_file_using_open_binary(excel_file_name, folder_name)
            excel_stream = BytesIO(excel_file)
            excel_file_df = pd.read_excel(io=excel_stream, sheet_name=SHEET_NAME)

            # Create a set of serial numbers from the Excel file
            serial_set = set(excel_file_df["Serial number"].tolist())
            print(f"Excel file retrieved. {len(excel_file_df)} rows found in existing sheet.")

        # Loop through all active submissions and transform them to the correct format
        print("STEP 4 - Looping submissions and mapping retrieved data to fit Excel column names.")
        for i, form in enumerate(all_submissions):
            form_serial_number = form["entity"]["serial"][0]["value"]

            # If the form's serial number is already in the Excel file, skip it
            if form_serial_number in serial_set:
                continue

            row_info = {}

            transformed_row = helper_functions.transform_form_submission(form_serial_number, form, formular_mapping)

            row_info["transformed_row"] = transformed_row

            if upload_pdfs_to_sharepoint_folder_name:
                row_info["upload_pdfs_to_sharepoint_folder_name"] = upload_pdfs_to_sharepoint_folder_name
                row_info["file_url"] = form["data"]["attachments"]["besvarelse_i_pdf_format"]["url"]

            new_submissions.append(row_info)

            if i == 2:
                break  # For testing, we only add one submission at a time

        work_item_data = {
            "os2_webform_id": os2_webform_id,
            "config": config,
            "submissions": new_submissions
        }

        queue_items.append(work_item_data)

        print(f"Added submissions for webform, {os2_webform_id}, to workqueue.\n")

    return queue_items

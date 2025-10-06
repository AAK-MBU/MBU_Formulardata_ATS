"""Module to hande queue population"""

import sys
import os
import asyncio
import logging
import json
import copy

from automation_server_client import Workqueue

import datetime

from io import BytesIO

import pandas as pd

from mbu_msoffice_integration.sharepoint_class import Sharepoint

from helpers import config
from helpers.config import WEBFORMS_CONFIG

from helpers import helper_functions

SHAREPOINT_SITE_URL = "https://aarhuskommune.sharepoint.com"
SHAREPOINT_DOCUMENT_LIBRARY = "Delte dokumenter"

TODAYS_DATE = datetime.date.today()

logger = logging.getLogger(__name__)


def retrieve_items_for_queue(sharepoint_kwargs: dict) -> list[dict]:
    """
    Function to populate the workqueue with items.
    """

    new_submissions = []
    queue_items = []

    db_conn_string = os.getenv("DBCONNECTIONSTRINGPROD")

    os2_webform_id = next(
        (key for key in WEBFORMS_CONFIG if f"--{key}" in sys.argv or key in sys.argv),
        None
    )

    if not os2_webform_id:
        raise ValueError("No matching form key found in sys.argv")

    form_config = WEBFORMS_CONFIG[os2_webform_id].copy()
    form_config = copy.deepcopy(WEBFORMS_CONFIG[os2_webform_id])

    logger.info(f"Webform_id: {os2_webform_id}")

    ### FOR DEV TESTING ONLY - OVERRIDE SITE AND FOLDER NAME TO AVOID POLLUTING ACTUAL FOLDERS ###
    # testing = True
    # if testing:
    #     form_config["site_name"] = "MBURPA"
    #     form_config["folder_name"] = "Automation_Server"
    #     if "upload_pdfs_to_sharepoint_folder_name" in form_config:
    #         form_config["upload_pdfs_to_sharepoint_folder_name"] = "Automation_Server/pdf"
    ### FOR DEV TESTING ONLY - OVERRIDE SITE AND FOLDER NAME TO AVOID POLLUTING ACTUAL FOLDERS ###

    site_name = form_config["site_name"]
    folder_name = form_config["folder_name"]
    excel_file_name = form_config["excel_file_name"]

    formular_mapping = form_config["formular_mapping"]
    del form_config["formular_mapping"]

    upload_pdfs_to_sharepoint_folder_name = form_config.get("upload_pdfs_to_sharepoint_folder_name", "")

    form_config["excel_file_exists"] = False

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

    logger.info("STEP 1 - Fetching all submissions")
    all_submissions = helper_functions.get_forms_data(
        conn_string=db_conn_string,
        form_type=os2_webform_id,
    )

    logger.info(f"OS2 submissions retrieved - {len(all_submissions)} total submissions found")

    if len(all_submissions) == 0:
        logger.info(f"There are no submissions for webform - {os2_webform_id}")

        return queue_items

    serial_set = set()

    logger.info("STEP 2 - Looking for existing excel file")
    try:
        files_in_sharepoint = sharepoint_api.fetch_files_list(folder_name=folder_name)
        file_names = [f["Name"] for f in files_in_sharepoint]

    except Exception as e:
        logger.info(f"Error when trying to fetch existing files in SharePoint: {e}")

    if excel_file_name in file_names:
        form_config["excel_file_exists"] = True

        # If the Excel file exists, we fetch it and load it into a DataFrame, so we can compare serial numbers
        excel_file = sharepoint_api.fetch_file_using_open_binary(
            excel_file_name,
            folder_name
        )

        excel_stream = BytesIO(excel_file)
        excel_file_df = pd.read_excel(io=excel_stream, sheet_name="Besvarelser")

        # Create a set of serial numbers from the Excel file
        serial_set = set(excel_file_df["Serial number"].tolist())
        logger.info(f"Excel file already exists - {len(excel_file_df)} rows found in existing sheet")

    # Loop through all active submissions and transform them to the correct format
    logger.info("STEP 3 - Looping submissions and identifying new ones to append")
    for form in all_submissions:
        form_serial_number = form["entity"]["serial"][0]["value"]

        # If the form's serial number is already in the Excel file, skip it
        if form_serial_number in serial_set:
            continue

        transformed_row = helper_functions.transform_form_submission(
            form_serial_number,
            form,
            formular_mapping
        )

        if upload_pdfs_to_sharepoint_folder_name:
            form_config["upload_pdfs_to_sharepoint_folder_name"] = upload_pdfs_to_sharepoint_folder_name
            form_config["file_url"] = form["data"]["attachments"]["besvarelse_i_pdf_format"]["url"]

        new_submissions.append(transformed_row)

    if len(new_submissions) > 0:
        logger.info(f"New submissions found: {len(new_submissions)}.")

        logger.info("STEP 4 - Appending work_item with new submissions to workqueue")
        work_item_data = {
            "reference": f"{os2_webform_id}_{TODAYS_DATE}",
            "data": {"config": form_config, "submissions": new_submissions},
        }

        queue_items.append(work_item_data)

    else:
        logger.info("No new submissions found.")

    return queue_items


def create_sort_key(item: dict) -> str:
    """
    Create a sort key based on the entire JSON structure.
    Converts the item to a sorted JSON string for consistent ordering.
    """
    return json.dumps(item, sort_keys=True, ensure_ascii=False)


async def concurrent_add(workqueue: Workqueue, items: list[dict]) -> None:
    """
    Populate the workqueue with items to be processed.
    Uses concurrency and retries with exponential backoff.

    Args:
        workqueue (Workqueue): The workqueue to populate.
        items (list[dict]): List of items to add to the queue.
        logger (logging.Logger): Logger for logging messages.

    Returns:
        None

    Raises:
        Exception: If adding an item fails after all retries.
    """
    sem = asyncio.Semaphore(config.MAX_CONCURRENCY)

    async def add_one(it: dict):
        reference = str(it.get("reference") or "")
        data = {"item": it}

        async with sem:
            for attempt in range(1, config.MAX_RETRIES + 1):
                try:
                    await asyncio.to_thread(workqueue.add_item, data, reference)
                    logger.info(f"Added item to queue with reference: {reference}")
                    return True

                except Exception as e:
                    if attempt >= config.MAX_RETRIES:
                        logger.error(
                            f"Failed to add item {reference} after {attempt} attempts: {e}"
                        )
                        return False

                    backoff = config.RETRY_BASE_DELAY * (2 ** (attempt - 1))

                    logger.warning(
                        f"Error adding {reference} (attempt {attempt}/{config.MAX_RETRIES}). "
                        f"Retrying in {backoff:.2f}s... {e}"
                    )
                    await asyncio.sleep(backoff)

    if not items:
        logger.info("No new items to add.")
        return

    sorted_items = sorted(items, key=create_sort_key)
    logger.info(
        f"Processing {len(sorted_items)} items sorted by complete JSON structure"
    )

    results = await asyncio.gather(*(add_one(i) for i in sorted_items))
    successes = sum(1 for r in results if r)
    failures = len(results) - successes

    logger.info(
        f"Summary: {successes} succeeded, {failures} failed out of {len(results)}"
    )

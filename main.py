"""
main.py
"""

import sys
import os

import asyncio

from dotenv import load_dotenv

from automation_server_client import AutomationServer, Workqueue, WorkItemError

from helper_functions import helper_functions

LINE_BREAK = "\n\n\n" + "-" * 125 + "\n\n\n"

load_dotenv()  # Loads variables from .env

DB_CONN_STRING = os.getenv("DbConnectionString")

ATS_URL = os.getenv("ATS_URL")
ATS_TOKEN = os.getenv("ATS_TOKEN")


async def populate_queue(workqueue: Workqueue):
    """
    Function to populate the workqueue with items.
    """

    print("Hello from populate workqueue!\n")

    webforms = [
        "basisteam_spoergeskema_til_fagpe",
        "basisteam_spoergeskema_til_forae",
        "henvisningsskema_til_klinisk_hyp",
        "spoergeskema_hypnoterapi_foer_fo",
        "opfoelgende_spoergeskema_hypnote",
        "foraelder_en_god_overgang_fra_hj",
        "fagperson_en_god_overgang_fra_hj",
        "sundung_aarhus",
        "tilmelding_til_modersmaalsunderv",
    ]

    for webform_id in webforms:
        if webform_id not in (
            "basisteam_spoergeskema_til_fagpe",
            # "henvisningsskema_til_klinisk_hyp",
            # "spoergeskema_hypnoterapi_foer_fo"
        ):
            continue

        print(f"Looping through submissions for webform_id: {webform_id}")

        all_forms = helper_functions.get_forms_data(
            conn_string=DB_CONN_STRING,
            form_type=webform_id,
        )

        for i, form in enumerate(all_forms):
            form_uuid = form["entity"]["uuid"][0]["value"]

            if form_uuid != "2a8cbe7b-64e8-4d3c-a3ff-fd0342b4fead":
                continue

            workqueue.add_item(
                data={
                    "webform_id": webform_id,
                    "data": form,
                },
                reference=form_uuid
            )

            print(f"Added form with reference: {form_uuid} to workqueue.\n")

        print(LINE_BREAK)


async def process_workqueue(workqueue: Workqueue):
    """
    Function to process the workqueue items.
    """

    print("Hello from process workqueue!")

    for item in workqueue:
        reference = item.reference

        data = item.get_data_as_dict()

        webform_id = data.get("webform_id")

        form_data = data.get("data")

        with item:
            try:
                # Process the item here
                print(f"Processing item with data: {data}")

            except WorkItemError as e:
                # A WorkItemError represents a soft error that indicates the item should be passed to manual processing or a business logic fault
                print(f"Error processing item: {data}. Error: {e}")

                item.fail(str(e))

        print()

if __name__ == "__main__":
    ats = AutomationServer.from_environment()

    test_workqueue = ats.workqueue()

    print("TEST PRINT")

    print(sys.argv)

    if "--queue" in sys.argv:
        print("Queue argument detected, clearing workqueue...")

        test_workqueue.clear_workqueue("new")

        asyncio.run(populate_queue(test_workqueue))

        sys.exit(0)

    asyncio.run(process_workqueue(test_workqueue))
    print("Workqueue processing completed.")

"""
This is the main entry point for the process
"""

import asyncio
import logging
import sys
import os

from dotenv import load_dotenv

from automation_server_client import AutomationServer, Workqueue

from mbu_dev_shared_components.database.connection import RPAConnection
from mbu_dev_shared_components.msoffice365.sharepoint_api.files import Sharepoint

from mbu_rpa_core.exceptions import BusinessError, ProcessError
from mbu_rpa_core.process_states import CompletedState

from processes.error_handling import send_error_email
from processes.finalize_process import finalize_process
from processes.item_retriever import item_retriever
from processes.process_item import process_item

load_dotenv()  # Loads variables from .env


### REMOVE IN PRODUCTION ###
import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

_old_request = requests.Session.request


def unsafe_request(self, *args, **kwargs):
    """
    TESTING PURPOSES ONLY - DISABLES SSL VERIFICATION FOR ALL REQUESTS
    """
    kwargs['verify'] = False
    return _old_request(self, *args, **kwargs)


requests.Session.request = unsafe_request
### REMOVE IN PRODUCTION ###


# TEMPORARY OVERRIDE: Set a new env variable in memory only
# os.environ["DbConnectionString"] = os.getenv("DBConnectionStringProd")

RPA_CONN = RPAConnection(db_env="PROD", commit=False)
with RPA_CONN:
    SCV_LOGIN = RPA_CONN.get_credential("SvcRpaMbu002")
    USERNAME = SCV_LOGIN.get("username", "")
    PASSWORD = SCV_LOGIN.get("decrypted_password", "")

    OS2_API_KEY = RPA_CONN.get_credential("os2_api").get("decrypted_password", "")

SHAREPOINT_FOLDER_URL = "https://aarhuskommune.sharepoint.com"
SHAREPOINT_DOCUMENT_LIBRARY = "Delte dokumenter"


async def populate_queue(workqueue: Workqueue, sharepoint_api: Sharepoint):
    """Populate the workqueue with items to be processed."""

    logger = logging.getLogger(__name__)

    logger.info("Hello from populate workqueue!")

    items_to_queue = item_retriever(workqueue=workqueue, sharepoint_api=sharepoint_api)

    for item in items_to_queue:
        reference = item.get("os2_webform_id")

        # config = item.get("config", {})

        work_item = workqueue.add_item(
            data=item,
            reference=reference
        )

        logger.info(f"Added item to queue: {work_item}")


async def process_workqueue(workqueue: Workqueue, sharepoint_api: Sharepoint, os2_api_key: str):
    """Process items from the workqueue."""

    logger = logging.getLogger(__name__)

    logger.info("Hello from process workqueue!")

    for item in workqueue:
        with item:
            data = item.data  # Item data deserialized from json as dict

            try:
                process_item(data, sharepoint_api=sharepoint_api, os2_api_key=os2_api_key)

                completed_state = CompletedState.completed("Process completed without exceptions")  # Adjust message for specific purpose
                item.complete(str(completed_state))

                continue

            except ProcessError as e:
                # A ProcessError indicates a problem with the RPA process to be handled by the RPA team
                logger.error(f"Error processing item: {data}. Error: {e}")

                item.fail(str(e))

                raise ProcessError from e

            except BusinessError as e:
                # A BusinessError indicates a breach of business logic or something else to be handled by business department
                logger.info(f"A BusinessError was raised for item: {data}. Error: {e}")

                item.pending_user(str(e))


async def finalize():
    """Finalize process."""

    logger = logging.getLogger(__name__)

    logger.info("Hello from finalize!")

    try:
        finalize_process()

    except ProcessError as e:
        # A ProcessError indicates a problem with the RPA process to be handled by the RPA team
        logger.error(f"Error when finalizing. Error: {e}")

        raise ProcessError from e

    except BusinessError as e:
        # A BusinessError indicates a breach of business logic or something else to be handled by business department
        logger.info(f"A BusinessError was raised during finalizing. Error: {e}")


if __name__ == "__main__":
    ats = AutomationServer.from_environment()

    prod_workqueue = ats.workqueue()
    process = ats.process

    sharepoint = Sharepoint(username=USERNAME, password=PASSWORD, site_url=SHAREPOINT_FOLDER_URL, site_name="", document_library=SHAREPOINT_DOCUMENT_LIBRARY)

    # Initialize external systems for automation here..
    try:
        # Queue management
        if "--queue" in sys.argv:
            asyncio.run(populate_queue(prod_workqueue, sharepoint_api=sharepoint))

            sys.exit(0)

        if "--process" in sys.argv:
            # Process workqueue
            asyncio.run(process_workqueue(prod_workqueue, sharepoint_api=sharepoint, os2_api_key=OS2_API_KEY))

        if "--finalize" in sys.argv:
            # Finalize process
            asyncio.run(finalize())

    except ProcessError as e:
        send_error_email(e, process_name=process.name)

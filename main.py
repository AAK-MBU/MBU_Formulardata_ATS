import asyncio
import logging
import sys
import os

import requests

from automation_server_client import AutomationServer, Workqueue, WorkItemError


async def populate_queue(workqueue: Workqueue):
    webforms = [
        "spoergeskema_hypnoterapi_foer_fo",
        "basisteam_spoergeskema_til_fagpe",
        # ...add all 6
    ]

    for form_id in webforms:
        workqueue.add_item(
            data={"os2_webform_id": form_id},
            reference=form_id  # can be anything unique
        )

    print("HELLO WORLD")


async def process_workqueue(workqueue: Workqueue):
    logger = logging.getLogger(__name__)

    logger.info("Hello from process workqueue!")

    for item in workqueue:
        with item:
            data = item.get_data_as_dict()

            try:
                # Process the item here
                pass

            except WorkItemError as e:
                # A WorkItemError represents a soft error that indicates the item should be passed to manual processing or a business logic fault
                logger.error(f"Error processing item: {data}. Error: {e}")

                item.fail(str(e))


print("TEST PRINT")


if __name__ == "__main__":
    ats = AutomationServer.from_environment()

    workqueue = ats.workqueue()

    # Initialize external systems for automation here..

    # Queue management
    if "--queue" in sys.argv:
        workqueue.clear_workqueue("new")

        asyncio.run(populate_queue(workqueue))

        exit(0)

    # Process workqueue
    asyncio.run(process_workqueue(workqueue))

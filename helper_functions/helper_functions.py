""" Script to upload fetch an OS2-formular submission and upload it in pdf format to Sharepoint. """
from urllib.parse import unquote, urlparse

import json
import urllib.parse

from typing import Dict, Any

from io import BytesIO

import math

import requests

import pandas as pd

import openpyxl

from sqlalchemy import create_engine

from openpyxl.styles import Alignment

from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection

from mbu_dev_shared_components.msoffice365.sharepoint_api.files import Sharepoint


def load_credential(url, token, credential_name: str) -> dict:
    """
    Fetch a credential object from the Automation Server using its name,
    and return the full credential dictionary with 'data' parsed into a dict.
    """

    if not url or not token:
        raise EnvironmentError("ATS_URL or ATS_TOKEN is not set in the environment")

    headers = {
        "Authorization": f"Bearer {token}"
    }

    full_url = f"{url}/credentials/by_name/{credential_name}"

    response = requests.get(full_url, headers=headers, timeout=60)

    response.raise_for_status()

    credential = response.json()

    # Parse the JSON string stored in the 'data' field
    try:
        credential["data"] = json.loads(credential.get("data", "{}"))

    except json.JSONDecodeError:
        credential["data"] = {}
        print(f"Warning: Failed to decode 'data' for credential '{credential_name}'.")

    return credential


def get_credentials_and_constants(orchestrator_connection: OrchestratorConnection) -> Dict[str, Any]:
    """Retrieve necessary credentials and constants from the orchestrator connection."""
    try:
        credentials = {
            "go_api_endpoint": orchestrator_connection.get_constant('go_api_endpoint').value,
            "go_api_username": orchestrator_connection.get_credential('go_api').username,
            "go_api_password": orchestrator_connection.get_credential('go_api').password,
            "os2_api_key": orchestrator_connection.get_credential('os2_api').password,
            "sql_conn_string": orchestrator_connection.get_constant('DbConnectionString').value,
            "journalizing_tmp_path": orchestrator_connection.get_constant('journalizing_tmp_path').value,
        }
        return credentials
    except AttributeError as e:
        raise SystemExit(e) from e


def get_forms_data(conn_string: str, form_type: str) -> list[dict]:
    """
    Retrieve form_data['data'] for all matching submissions for the given form type,
    excluding purged entries.
    """

    query = """
        SELECT
            form_id,
            form_data,
            CAST(form_submitted_date AS datetime) AS form_submitted_date
        FROM
            [RPA].[journalizing].[Forms]
        WHERE
            form_type = ?
            AND form_data IS NOT NULL
            AND form_submitted_date IS NOT NULL
        ORDER BY form_submitted_date DESC
    """

    # Create SQLAlchemy engine
    encoded_conn_str = urllib.parse.quote_plus(conn_string)
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={encoded_conn_str}")

    try:
        df = pd.read_sql(sql=query, con=engine, params=(form_type,))

    except Exception as e:
        print("Error during pd.read_sql:", e)

        raise

    if df.empty:
        print("No submissions found for the given form type.")

        return []

    extracted_data = []
    for _, row in df.iterrows():
        try:
            parsed = json.loads(row["form_data"])
            if "purged" not in parsed:  # Skip purged entries
                extracted_data.append(parsed)

        except json.JSONDecodeError:
            print("Invalid JSON in form_data, skipping row.")

    return extracted_data


def format_excel_file(excel_stream: BytesIO) -> BytesIO:
    """
    Applies formatting to an Excel file contained in a BytesIO stream.
    This includes:
      - Freezing the first row.
      - Applying left and top alignment to all cells.
      - Auto-adjusting column widths up to a maximum width and enabling wrap_text if needed.
      - Auto-adjusting row heights based on the wrapped text.

    Returns:
        A new BytesIO stream containing the formatted workbook.
    """

    # Load the workbook from the input stream
    wb = openpyxl.load_workbook(excel_stream)
    ws = wb.active

    # Freeze the first row
    ws.freeze_panes = "A2"

    # Apply left alignment and top vertical alignment to all cells
    for row in ws.iter_rows():
        for cell in row:
            cell.alignment = Alignment(horizontal="left", vertical="top")

    # Define a maximum column width (in characters)
    max_allowed_width = 100  # adjust as needed

    # Auto-adjust column widths based on content length, enabling wrap_text if necessary
    for col in ws.columns:
        max_length = 0

        column_letter = col[0].column_letter  # Get column letter

        for cell in col:
            if cell.value:
                max_length = max(max_length, len(str(cell.value)))

        computed_width = max_length + 2

        if computed_width > max_allowed_width:
            ws.column_dimensions[column_letter].width = max_allowed_width

            # Enable wrap_text for cells in this column
            for cell in col:
                cell.alignment = Alignment(horizontal="left", vertical="top", wrap_text=True)

        else:
            ws.column_dimensions[column_letter].width = computed_width

    # Auto-adjust row heights based on wrapped text (simulate double-click auto-fit)
    for row in ws.iter_rows():
        max_line_count = 1  # Start with at least one line

        for cell in row:
            if cell.value and cell.alignment.wrap_text:
                col_letter = cell.column_letter

                # Use the set column width or a default value if not set
                col_width = ws.column_dimensions[col_letter].width or 10

                # Estimate how many characters fit in one line (factor may need tweaking)
                chars_per_line = col_width * 1.2

                # Split the cell text by newlines
                lines = str(cell.value).split("\n")

                # Estimate total line count for the cell
                line_count = sum(math.ceil(len(line) / chars_per_line) for line in lines)

                max_line_count = max(max_line_count, line_count)

        # Set the row height (multiplier of 20 is a rough estimate; adjust as needed)
        ws.row_dimensions[row[0].row].height = max_line_count * 20

    # Save the formatted workbook to a new BytesIO stream and return it
    formatted_stream = BytesIO()

    wb.save(formatted_stream)

    formatted_stream.seek(0)

    return formatted_stream


def upload_pdf_to_sharepoint(
    orchestrator_connection: OrchestratorConnection,
    sharepoint_api: Sharepoint,
    folder_name: str,
    os2_api_key: str,
    active_forms: list,
) -> None:
    """Main function to upload a PDF to Sharepoint."""

    orchestrator_connection.log_trace("Upload PDF to Sharepoint started.")
    print("Upload PDF to Sharepoint started.")

    existing_pdfs_sum = 0

    existing_pdfs = sharepoint_api.fetch_files_list(folder_name=folder_name)

    if existing_pdfs:
        existing_pdf_names = {file["Name"] for file in existing_pdfs}

    else:
        existing_pdf_names = set()

    for form in active_forms:
        file_url = form["data"]["attachments"]["besvarelse_i_pdf_format"]["url"]

        path = urlparse(file_url).path
        filename = path.split('/')[-1]
        final_filename = f"{unquote(filename)}"

        print(file_url)
        print(final_filename)

        if final_filename in existing_pdf_names:
            print(f"File {final_filename} already exists in Sharepoint. Skipping download.")

            existing_pdfs_sum += 1

            continue

        orchestrator_connection.log_trace("Downloading PDF from OS2Forms API.")
        print("Downloading PDF from OS2Forms API.")
        try:
            downloaded_file = download_file_bytes(file_url, os2_api_key)

        except requests.RequestException as error:
            orchestrator_connection.log_trace(f"Failed to download file: {error}")
            print(f"Failed to download file: {error}")

        # Upload the file to Sharepoint
        sharepoint_api.upload_file_from_bytes(
            binary_content=downloaded_file,
            file_name=final_filename,
            folder_name=folder_name
        )

    if existing_pdfs_sum == len(active_forms):
        orchestrator_connection.log_trace("All files already exist in Sharepoint. No new files uploaded.")
        print("All files already exist in Sharepoint. No new files uploaded.")


def download_file_bytes(url: str, os2_api_key: str) -> bytes:
    """Downloads the content of a file from a specified URL, appending an API key to the URL for authorization.
    The API key is retrieved from an environment variable 'OS2ApiKey'.

    Parameters:
    url (str): The URL from which the file will be downloaded.
    os2_api_key (str): The API-key for OS2Forms api.

    Returns:
    bytes: The content of the file as a byte stream.

    Raises:
    requests.RequestException: If the HTTP request fails for any reason.
    """

    headers = {
        'Content-Type': 'application/json',
        'api-key': f'{os2_api_key}'
    }

    response = requests.request(method='GET', url=url, headers=headers, timeout=60)

    response.raise_for_status()

    return response.content

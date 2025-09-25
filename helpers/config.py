"""Module for general configurations of the process"""

from helpers import formular_mappings

# ----------------------
# Workqueue settings
# ----------------------
MAX_RETRY = 1

# ----------------------
# Queue population settings
# ----------------------
MAX_CONCURRENCY = 10  # tune based on backend capacity
MAX_RETRIES = 1  # failure retries per item
RETRY_BASE_DELAY = 0.5  # seconds

WEBFORMS_CONFIG = {
    "henvisningsskema_til_klinisk_hyp": {
        "excel_file_name": "Dataudtræk henvisningsskema hypnoterapi.xlsx",
        "folder_name": "General/Udtræk OS2Forms/Henvisningsskema",
        "formular_mapping": formular_mappings.henvisningsskema_til_klinisk_hyp_mapping,
        "site_name": "tea-teamsite10693",
    },
    "basisteam_spoergeskema_til_fagpe": {
        "excel_file_name": "Dataudtræk basisteam - fagperson.xlsx",
        "folder_name": "General/Evaluering/Udtræk OS2Forms",
        "formular_mapping": formular_mappings.basisteam_spoergeskema_til_fagpe_mapping,
        "site_name": "tea-teamsite8906",
        "upload_pdfs_to_sharepoint_folder_name": "General/Evaluering/Besvarelser fra OS2Forms - fagpersoner",
    },
    "basisteam_spoergeskema_til_forae": {
        "excel_file_name": "Dataudtræk basisteam - forældre.xlsx",
        "folder_name": "General/Evaluering/Udtræk OS2Forms",
        "formular_mapping": formular_mappings.basisteam_spoergeskema_til_forae_mapping,
        "site_name": "tea-teamsite8906",
        "upload_pdfs_to_sharepoint_folder_name": "General/Evaluering/Besvarelser fra OS2Forms - forældre",
    },
    # "spoergeskema_hypnoterapi_foer_fo": {
    #     "excel_file_name": "Dataudtræk spørgeskema hypnoterapi.xlsx",
    #     "folder_name": "General/Udtræk OS2Forms/Spørgeskema",
    #     "formular_mapping": formular_mappings.spoergeskema_hypnoterapi_foer_fo_mapping,
    #     "site_name": "tea-teamsite10693",
    # },
    # "opfoelgende_spoergeskema_hypnote": {
    #     "excel_file_name": "Dataudtræk opfølgende spørgeskema hypnoterapi.xlsx",
    #     "folder_name": "General/Udtræk OS2Forms/Opfølgende spørgeskema",
    #     "formular_mapping": formular_mappings.opfoelgende_spoergeskema_hypnote_mapping,
    #     "site_name": "tea-teamsite10693",
    # },
    # "foraelder_en_god_overgang_fra_hj": {
    #     "excel_file_name": "Dataudtræk en god overgang fra hjem til dagtilbud - forælder.xlsx",
    #     "folder_name": "General/Udtræk data OS2Forms/Opfølgende spørgeskema forældre",
    #     "formular_mapping": formular_mappings.foraelder_en_god_overgang_fra_hj_mapping,
    #     "site_name": "tea-teamsite10533",
    # },
    # "fagperson_en_god_overgang_fra_hj": {
    #     "excel_file_name": "Dataudtræk en god overgang fra hjem til dagtilbud - fagperson.xlsx",
    #     "folder_name": "General/Udtræk data OS2Forms/Opfølgende spørgeskema fagpersonale",
    #     "formular_mapping": formular_mappings.fagperson_en_god_overgang_fra_hj_mapping,
    #     "site_name": "tea-teamsite10533",
    # },
    # "sundung_aarhus": {
    #     "excel_file_name": "Dataudtræk SundUng Aarhus.xlsx",
    #     "folder_name": "General/Udtræk OS2-formularer",
    #     "formular_mapping": formular_mappings.sundung_aarhus_mapping,
    #     "site_name": "tea-teamsite11121",
    # },
    # "tilmelding_til_modersmaalsunderv": {
    #     "excel_file_name": f"Dataudtræk - {monday_last_week} til {sunday_last_week}.xlsx",
    #     "folder_name": "General",
    #     "formular_mapping": formular_mappings.tilmelding_til_modersmaalsunderv_mapping,
    #     "site_name": "Teams-Modersmlsundervisning",
    # },
}

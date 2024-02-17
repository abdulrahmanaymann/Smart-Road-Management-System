import pandas as pd
from Config.config import *
from Config.Logger import LOGGER


def read_excel_sheets(file_path):
    try:
        # Read all excel sheets into a dictionary
        sheets = pd.read_excel(file_path, sheet_name=[SHEET1, SHEET2, SHEET3, SHEET4])
        return sheets[SHEET1], sheets[SHEET2], sheets[SHEET3], sheets[SHEET4]
    except FileNotFoundError as e:
        LOGGER.error(f"Error: File '{file_path}' not found: {e}")
        return None, None, None, None
    except Exception as e:
        LOGGER.error(f"Error: {e}")
        return None, None, None, None

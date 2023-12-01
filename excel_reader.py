import pandas as pd
from config import *


def read_excel_sheets(file_path):
    # Read all excel sheets into a dictionary
    sheets = pd.read_excel(file_path, sheet_name=[SHEET1, SHEET2, SHEET3, SHEET4])
    return sheets[SHEET1], sheets[SHEET2], sheets[SHEET3], sheets[SHEET4]
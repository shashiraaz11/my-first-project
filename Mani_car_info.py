import os
import json
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime

def import_car_data():
    # Google Sheets API scope
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

    # Service account credentials from ENV variable
    key_data = os.environ.get("ACCOUNT_KEY_JSON")
    if not key_data:
        raise Exception("ACCOUNT_KEY_JSON environment variable not found")

    creds_dict = json.loads(key_data)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    client = gspread.authorize(creds)

    # Spreadsheet IDs and sheet names
    source_spreadsheet_id = "1yDoXBuatVAep4z47L-WbSYvEELKZ3VOJm1CWwSQdWkU"
    source_sheet_names = ["Car Info from CNG", "Car Info from EV"]
    target_spreadsheet_id = "1LYtmHJ3NOGs0Likkl7_eIfemX-g9kVGhfIN1FzMGBh4"
    target_sheet_name = "Info Data"
    last_run_sheet_name = "Last Script Run"

    all_processed_data = []

    for sheet_name in source_sheet_names:
        source = client.open_by_key(source_spreadsheet_id).worksheet(sheet_name)
        data = source.get("B:L")

        # EV sheet ke liye first row (header) skip karenge
        if sheet_name == "Car Info from EV" and len(data) > 1:
            data = data[1:]

        # Process data
        for row in data:
            if len(row) > 1 and row[1]:
                all_processed_data.append([
                    row[0],   # loc_id
                    row[1],   # partner_etm
                    row[4] if len(row) > 4 else "",  # start_date
                    row[5] if len(row) > 5 else "",  # end_date
                    row[6] if len(row) > 6 else "",  # allocation_date
                    row[7] if len(row) > 7 else "",  # car_type
                    row[8] if len(row) > 8 else "",  # business_vertical
                    row[10] if len(row) > 10 else "", # extra col
                ])

    # Open target sheet
    target = client.open_by_key(target_spreadsheet_id).worksheet(target_sheet_name)

    # Clear only A:H
    target.batch_clear(["A:H"])

    # Write processed data
    if all_processed_data:
        target.update("A1", all_processed_data, value_input_option="USER_ENTERED")

    # Update last run sheet
    try:
        last_run = client.open_by_key(target_spreadsheet_id).worksheet(last_run_sheet_name)
        last_run.clear()
    except:
        last_run = client.open_by_key(target_spreadsheet_id).add_worksheet(last_run_sheet_name, rows=10, cols=5)

    last_run.update("A1", [["Last Script Run Time", datetime.now().strftime("%Y-%m-%d %H:%M:%S")]])

    print(f" Imported {len(all_processed_data)} rows from both sheets (EV header skipped).")

if __name__ == "__main__":
    import_car_data()

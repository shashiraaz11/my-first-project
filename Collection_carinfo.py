import os
import json
import gspread
from google.oauth2.service_account import Credentials

# Setup Google Sheets authentication from environment variable
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
key_data = os.environ.get("ACCOUNT_KEY_JSON")

if not key_data:
    raise Exception("ACCOUNT_KEY_JSON environment variable not found")

creds_dict = json.loads(key_data)
creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
client = gspread.authorize(creds)


# Source and target Google Sheets
source_sheet_id = "1LYtmHJ3NOGs0Likkl7_eIfemX-g9kVGhfIN1FzMGBh4"
target_sheet_id = "1HMlQzPbqpEh2OiIZT6h5UxjfY-wmWUrLQDgahNsxzl0"
source_tab_name = "Car Info"
target_tab_name = "Details"

# Open the source sheet and fetch data from A2:K (removing apostrophes)
print("Fetching data from A2:K in source sheet...")
source_sheet = client.open_by_key(source_sheet_id).worksheet(source_tab_name)
data = source_sheet.get("A2:K", value_render_option='UNFORMATTED_VALUE')  # Fetch as raw data

if data:
    print(f"Fetched {len(data)} rows from A2:K.")

    # Open the target sheet
    print("Opening target sheet...")
    target_sheet = client.open_by_key(target_sheet_id).worksheet(target_tab_name)

    # Clear existing content in the range
    print("Clearing target sheet from A2:K...")
    target_sheet.batch_clear(["A2:K"])

    # Write values to A2:AA in the target sheet
    print("Writing data to target sheet...")
    target_sheet.update(values=data, range_name="A2")  # Write clean data

    print("Data successfully transferred!")

else:
    print("No data found in A2:K.")

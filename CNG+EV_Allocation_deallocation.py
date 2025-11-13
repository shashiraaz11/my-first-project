import os
import json
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import pytz

# =============================
# 🔐 Authentication Setup
# =============================
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
key_data = os.environ.get("ACCOUNT_KEY_JSON")

if not key_data:
    raise Exception("ACCOUNT_KEY_JSON environment variable not found")

creds_dict = json.loads(key_data)
creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
client = gspread.authorize(creds)

# ==================================
# 🔗 Sheet IDs (EV and CNG)
# ==================================
source_ev_sheet_id = "1pq2VwJzF-YASo3VIIxHRW-SpuWD_mvAngZvTRcWGxkg"
source_cng_sheet_id = "1jELPAsR-2zlz4182S7Byc5WmqtLa-ZYjIRD_hyRXBAo"
target_sheet_id = "1yDoXBuatVAep4z47L-WbSYvEELKZ3VOJm1CWwSQdWkU"

# ==================================
# 📤 Transfer Function
# ==================================
def transfer_tab(source_sheet_id, source_tab_name, target_tab_name, data_range):
    try:
        print(f"⚙️ Processing tab '{source_tab_name}' → '{target_tab_name}'...")

        source_sheet = client.open_by_key(source_sheet_id).worksheet(source_tab_name)
        data = source_sheet.get(data_range, value_render_option='UNFORMATTED_VALUE')

        if data:
            print(f"📥 Fetched {len(data)} rows from {data_range}.")
            target_sheet = client.open_by_key(target_sheet_id).worksheet(target_tab_name)

            print(f"🧹 Clearing target sheet '{target_tab_name}' range {data_range}...")
            target_sheet.batch_clear([data_range])

            print(f"📤 Writing data to target sheet '{target_tab_name}'...")
            target_sheet.update(data_range.split(":")[0] + "1", data)

            print(f"✅ Data transferred to '{target_tab_name}'.\n")
        else:
            print(f"⚠️ No data found in {data_range} of tab '{source_tab_name}'.\n")

    except Exception as e:
        print(f"❌ Error transferring '{source_tab_name}': {e}\n")

# ==================================
# 🕒 Log Last Run
# ==================================
def log_last_run(script_name):
    try:
        tz = pytz.timezone("Asia/Kolkata")
        now = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
        sheet = client.open_by_key(target_sheet_id).worksheet("last script run")
        sheet.update("A1", [["Script Name", "Last Run Time"]])
        sheet.update("A2", [[script_name, now]])
        print(f"🕒 Logged run for '{script_name}' at {now}")
    except Exception as e:
        print(f"⚠️ Error logging run: {e}")

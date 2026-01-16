import os
import json
import gspread
from google.oauth2.service_account import Credentials

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

key_data = os.environ.get("ACCOUNT_KEY_JSON")
if not key_data:
    raise Exception("ACCOUNT_KEY_JSON env variable missing")

creds = Credentials.from_service_account_info(
    json.loads(key_data),
    scopes=SCOPES
)
client = gspread.authorize(creds)

SOURCE_SHEET_ID = "1yDoXBuatVAep4z47L-WbSYvEELKZ3VOJm1CWwSQdWkU"
TARGET_SHEET_ID = "1fS8creQX5JyxMVeSQmsRzd0Tfm4j4m8-mZW6l6Athc4"

SOURCE_TAB = "Combined CNG"
TARGET_TAB = "Combined"

# Source: A2 to AL (no header, no faltu)
source_ws = client.open_by_key(SOURCE_SHEET_ID).worksheet(SOURCE_TAB)
data = source_ws.get("A2:AL", value_render_option="UNFORMATTED_VALUE")

if not data:
    print("Kuch bhi data nahi mila 🤷")
    exit()

# Target: clear + write exactly A:AL
target_ws = client.open_by_key(TARGET_SHEET_ID).worksheet(TARGET_TAB)
target_ws.batch_clear(["A:AL"])
target_ws.update(range_name="A2:AL", values=data)

print("Perfect 👍 A:AL → A:AL transfer ho gaya")

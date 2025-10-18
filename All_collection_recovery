import gspread
from google.oauth2.service_account import Credentials
import os

# --- Authenticate using GitHub secret ---
account_key_json = os.environ.get("ACCOUNT_KEY_JSON")
if not account_key_json:
    raise FileNotFoundError("❌ ACCOUNT_KEY_JSON environment variable missing!")

import json
creds_dict = json.loads(account_key_json)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
credentials = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
gc = gspread.authorize(credentials)

# --- File IDs ---
SOURCE_ID = "1D4LjhxfaBpV1zUSCrQ7Xfe2NpeNRNgSdli16lh4anlo"
TARGET_ID = "1sipU5ThP9PmJYBBn06XxGZkPvUNobBCHQWo8jNwUyuw"

# --- 1️⃣ Function: ossummarycollection ---
def ossummarycollection():
    print("▶️ Running ossummarycollection...")

    source = gc.open_by_key(SOURCE_ID).worksheet("OS_ETM_Summary")
    target = gc.open_by_key(TARGET_ID).worksheet("OS_Collection")

    data = source.get_all_values()
    if len(data) < 3:
        print("⚠️ No data to copy.")
        return

    headers = data[2]
    rows = data[3:]

    filtered = [r for r in rows if r[1] in ["Delhi NCR", "Sukhrali", "Noida", "Delhi"]]

    target.batch_clear(["A:Q"])
    target.insert_row(headers, 1)
    if filtered:
        target.update(f"A2:Q{len(filtered)+1}", filtered)

    print(f"✅ Copied {len(filtered)} rows to OS_Collection.")


# --- 2️⃣ Function: updateRecovery ---
def updateRecovery():
    print("▶️ Running updateRecovery...")

    source = gc.open_by_key(SOURCE_ID)
    target = gc.open_by_key(TARGET_ID)

    leasing = source.worksheet("Leasing_Raw").get_all_values()
    revshare = source.worksheet("Revshare_Raw").get_all_values()
    target_ws = target.worksheet("Recovery")

    target_ws.batch_clear(["A:G"])

    # Part 1: Leasing_Raw → A:G
    target_ws.update(f"A1:G{len(leasing)}", leasing)

    # Part 2: Revshare_Raw → filtered & remapped
    filtered = [r for i, r in enumerate(revshare) if i == 0 or r[0] != ""]
    final = [[r[0], r[1], r[3], r[4], r[5], r[6], r[7]] for r in filtered]
    start_row = len(leasing) + 3
    target_ws.update(f"A{start_row}:G{start_row + len(final) - 1}", final)

    print("✅ Recovery sheet updated successfully.")


# --- Run both ---
if __name__ == "__main__":
    ossummarycollection()
    updateRecovery()
    print("🎉 All tasks done successfully!")

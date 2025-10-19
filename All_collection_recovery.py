import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
import os, json

# ===== AUTH SETUP =====
def get_gsheet_client():
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
    key_data = os.environ.get("ACCOUNT_KEY_JSON")

    if not key_data:
        raise FileNotFoundError("❌ No ACCOUNT_KEY_JSON found in environment variables.")

    creds_dict = json.loads(key_data)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    client = gspread.authorize(creds)
    return client

# ===== OS SUMMARY COLLECTION =====
def ossummarycollection():
    print("▶️ Running ossummarycollection...")

    # IDs
    source_id = "1D4LjhxfaBpV1zUSCrQ7Xfe2NpeNRNgSdli16lh4anlo"
    target_id = "1sipU5ThP9PmJYBBn06XxGZkPvUNobBCHQWo8jNwUyuw"

    # Tabs
    source_tab = "OS_ETM_Summary"
    target_tab = "OS_Collection"

    client = get_gsheet_client()
    source = client.open_by_key(source_id).worksheet(source_tab)
    target = client.open_by_key(target_id).worksheet(target_tab)

    # Get A:Q only
    data = source.get("A:Q", value_render_option="UNFORMATTED_VALUE")

    if len(data) <= 2:
        print("⚠️ No data found to copy.")
        return

    headers = data[2]  # Row 3 = headers
    rows = data[3:]    # Row 4 onwards = data

    # Filter rows (Col B)
    filtered = [
        row for row in rows
        if len(row) > 1 and str(row[1]).strip().lower() in ["delhi ncr", "sukhrali", "noida", "delhi"]
    ]

    print(f"✅ Filtered rows: {len(filtered)}")

    # Clear old data
    target.batch_clear(["A:Q"])

    # Write new data
    target.update("A1:Q1", [headers])
    if filtered:
        target.update(f"A2:Q{len(filtered)+1}", filtered)

    print("✅ OS Collection updated successfully!\n")

# ===== RECOVERY UPDATE =====
def updateRecovery():
    print("▶️ Running updateRecovery...")

    source_id = "1D4LjhxfaBpV1zUSCrQ7Xfe2NpeNRNgSdli16lh4anlo"
    target_id = "1sipU5ThP9PmJYBBn06XxGZkPvUNobBCHQWo8jNwUyuw"

    # Tabs
    leasing_tab = "Leasing_Raw"
    revshare_tab = "Revshare_Raw"
    target_tab = "Recovery"

    client = get_gsheet_client()
    source = client.open_by_key(source_id)
    target = client.open_by_key(target_id)

    leasing_sheet = source.worksheet(leasing_tab)
    revshare_sheet = source.worksheet(revshare_tab)
    recovery_sheet = target.worksheet(target_tab)

    # Clear old data (A:G)
    recovery_sheet.batch_clear(["A:G"])

    # Leasing Data (A:G)
    leasing_data = leasing_sheet.get("A:G", value_render_option="UNFORMATTED_VALUE")

    if not leasing_data:
        print("⚠️ No leasing data found.")
        return

    recovery_sheet.update("A1:G" + str(len(leasing_data)), leasing_data)
    print(f"✅ Leasing data copied ({len(leasing_data)} rows)")

    # Revshare Data (A:H)
    rev_data = revshare_sheet.get("A:H", value_render_option="UNFORMATTED_VALUE")
    if not rev_data:
        print("⚠️ No revshare data found.")
        return

    # Keep header + rows where Col1 not blank
    filtered = [row for i, row in enumerate(rev_data) if i == 0 or row[0] != ""]
    # Columns: A,B,D,E,F,G,H -> 0,1,3,4,5,6,7
    final_data = [[r[0], r[1], r[3], r[4], r[5], r[6], r[7]] for r in filtered]

    # Append below leasing data (2 blank rows)
    start_row = len(leasing_data) + 3
    recovery_sheet.update(f"A{start_row}:G{start_row + len(final_data) - 1}", final_data)

    print(f"✅ Revshare data appended ({len(final_data)} rows)\n")
    print("🎯 Recovery sheet updated successfully!")

# ===== MAIN =====
if __name__ == "__main__":
    print("🚀 Starting All_collection_recovery.py...")
    ossummarycollection()
    updateRecovery()
    print("✅ All tasks completed successfully!")

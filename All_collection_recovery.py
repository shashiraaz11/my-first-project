import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
import os, json
from datetime import datetime

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


# ===== 1️⃣ OS SUMMARY COLLECTION =====
def ossummarycollection(client):
    print("▶️ Running ossummarycollection...")

    source_id = "1D4LjhxfaBpV1zUSCrQ7Xfe2NpeNRNgSdli16lh4anlo"
    target_id = "1sipU5ThP9PmJYBBn06XxGZkPvUNobBCHQWo8jNwUyuw"

    source_tab = "OS_ETM_Summary"
    target_tab = "OS_Collection"

    source = client.open_by_key(source_id).worksheet(source_tab)
    target = client.open_by_key(target_id).worksheet(target_tab)

    data = source.get("A:Q", value_render_option="UNFORMATTED_VALUE")

    if len(data) <= 2:
        print("⚠️ No data found to copy.")
        return

    headers = data[2]
    rows = data[3:]

    filtered = [
        row for row in rows
        if len(row) > 1 and str(row[1]).strip().lower() in ["delhi ncr", "sukhrali", "noida", "delhi"]
    ]

    print(f"✅ Filtered rows: {len(filtered)}")

    target.batch_clear(["A:Q"])
    target.update("A1:Q1", [headers])
    if filtered:
        target.update(f"A2:Q{len(filtered)+1}", filtered)

    print("✅ OS Collection updated successfully!\n")


# ===== 2️⃣ RECOVERY UPDATE =====
def updateRecovery(client):
    print("▶️ Running updateRecovery...")

    source_id = "1D4LjhxfaBpV1zUSCrQ7Xfe2NpeNRNgSdli16lh4anlo"
    target_id = "1sipU5ThP9PmJYBBn06XxGZkPvUNobBCHQWo8jNwUyuw"

    leasing_tab = "Leasing_Raw"
    revshare_tab = "Revshare_Raw"
    target_tab = "Recovery"

    source = client.open_by_key(source_id)
    target = client.open_by_key(target_id)

    leasing_sheet = source.worksheet(leasing_tab)
    revshare_sheet = source.worksheet(revshare_tab)
    recovery_sheet = target.worksheet(target_tab)

    recovery_sheet.batch_clear(["A:G"])
    leasing_data = leasing_sheet.get("A:G", value_render_option="UNFORMATTED_VALUE")

    if not leasing_data:
        print("⚠️ No leasing data found.")
        return

    recovery_sheet.update("A1:G" + str(len(leasing_data)), leasing_data)
    print(f"✅ Leasing data copied ({len(leasing_data)} rows)")

    rev_data = revshare_sheet.get("A:H", value_render_option="UNFORMATTED_VALUE")
    if not rev_data:
        print("⚠️ No revshare data found.")
        return

    filtered = [row for i, row in enumerate(rev_data) if i == 0 or row[0] != ""]
    final_data = [[r[0], r[1], r[3], r[4], r[5], r[6], r[7]] for r in filtered]

    start_row = len(leasing_data) + 3
    recovery_sheet.update(f"A{start_row}:G{start_row + len(final_data) - 1}", final_data)

    print(f"✅ Revshare data appended ({len(final_data)} rows)\n")
    print("🎯 Recovery sheet updated successfully!")


# ===== 3️⃣ IMPORT CNG OS COLLECTION FAST =====
def importCNGOSCollectionFast(client):
    print("▶️ Running importCNGOSCollectionFast...")

    target_id = "1D4LjhxfaBpV1zUSCrQ7Xfe2NpeNRNgSdli16lh4anlo"
    target_tab = "CNG_OS_Summary"

    source_id = "1sipU5ThP9PmJYBBn06XxGZkPvUNobBCHQWo8jNwUyuw"
    source_tab = "OS_Collection"

    target = client.open_by_key(target_id).worksheet(target_tab)
    source = client.open_by_key(source_id).worksheet(source_tab)

    # Get filter date from E1
    filter_date_val = target.acell("E1").value
    if not filter_date_val:
        print("⚠️ No date in E1 cell.")
        return

    try:
        filter_date = datetime.strptime(filter_date_val, "%d/%m/%Y").date()
    except:
        try:
            filter_date = datetime.strptime(filter_date_val, "%Y-%m-%d").date()
        except:
            print("⚠️ Date format in E1 invalid.")
            return

    data = source.get("A:R", value_render_option="UNFORMATTED_VALUE")
    if not data or len(data) < 2:
        print("⚠️ No source data found.")
        return

    output = []
    for i, row in enumerate(data):
        if i == 0:
            continue
        if len(row) < 18 or not row[0]:
            continue

        try:
            row_date = datetime.strptime(str(row[0])[:10], "%Y-%m-%d").date()
        except:
            continue

        if row_date == filter_date and str(row[1]).strip() != "Delhi NCR":
            # Pick selected columns: D, A, B, C, F, E, K, Q, R
            new_row = [row[3], row[0], row[1], row[2], row[5], row[4], row[10], row[16], row[17]]
            output.append(new_row)

    # Clear old data in E2 onwards
    target.batch_clear(["E2:M"])
    if output:
        target.update(f"E2:M{len(output)+1}", output)
        print(f"✅ CNG OS Summary updated. Rows: {len(output)}")
    else:
        print("⚠️ No matching data found.")

    print("✅ importCNGOSCollectionFast completed!\n")


# ===== MAIN =====
if __name__ == "__main__":
    print("🚀 Starting All_collection_recovery.py...")
    client = get_gsheet_client()
    ossummarycollection(client)
    updateRecovery(client)
    importCNGOSCollectionFast(client)
    print("✅ All tasks completed successfully! 🎉")

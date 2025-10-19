import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from datetime import datetime
import os, json

# ===== AUTH SETUP =====
def get_gsheet_client():
    """
    Returns an authorized gspread client.
    Works both locally (file) and in GitHub Actions (env variable).
    """
    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    key_data = os.environ.get("ACCOUNT_KEY_JSON")
    SERVICE_ACCOUNT_FILE = r"C:\Users\skuma\Desktop\Python\Account key.json"

    creds = None

    if key_data:
        print("🔐 Using service account from environment variable (GitHub Actions)")
        try:
            creds_dict = json.loads(key_data)
            creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
            print("✅ Service account loaded from secret.")
        except Exception as e:
            raise ValueError(f"❌ Failed to load service account from env: {e}")

    elif os.path.exists(SERVICE_ACCOUNT_FILE):
        print("💾 Using local service account file")
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

    else:
        raise FileNotFoundError("❌ Service account credentials not found.")

    client = gspread.authorize(creds)
    return client


# ===== OS SUMMARY COLLECTION =====
def ossummarycollection():
    print("\n▶️ Running ossummarycollection...")
    try:
        source_id = "1D4LjhxfaBpV1zUSCrQ7Xfe2NpeNRNgSdli16lh4anlo"
        target_id = "1sipU5ThP9PmJYBBn06XxGZkPvUNobBCHQWo8jNwUyuw"
        source_tab = "OS_ETM_Summary"
        target_tab = "OS_Collection"

        client = get_gsheet_client()
        source = client.open_by_key(source_id).worksheet(source_tab)
        target = client.open_by_key(target_id).worksheet(target_tab)

        data = source.get("A:Q", value_render_option="UNFORMATTED_VALUE")
        if len(data) <= 2:
            print("⚠️ No data found to copy.")
            return

        headers = data[2]  # Row 3 = headers
        rows = data[3:]    # Row 4 onwards

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
    except Exception as e:
        print(f"❌ ossummarycollection failed: {e}")


# ===== RECOVERY UPDATE =====
def updateRecovery():
    print("\n▶️ Running updateRecovery...")
    try:
        source_id = "1D4LjhxfaBpV1zUSCrQ7Xfe2NpeNRNgSdli16lh4anlo"
        target_id = "1sipU5ThP9PmJYBBn06XxGZkPvUNobBCHQWo8jNwUyuw"

        leasing_tab = "Leasing_Raw"
        revshare_tab = "Revshare_Raw"
        target_tab = "Recovery"

        client = get_gsheet_client()
        source = client.open_by_key(source_id)
        target = client.open_by_key(target_id)

        leasing_sheet = source.worksheet(leasing_tab)
        revshare_sheet = source.worksheet(revshare_tab)
        recovery_sheet = target.worksheet(target_tab)

        # Clear old data
        recovery_sheet.batch_clear(["A:G"])

        # Copy leasing data
        leasing_data = leasing_sheet.get("A:G", value_render_option="UNFORMATTED_VALUE")
        if not leasing_data:
            print("⚠️ No leasing data found.")
        else:
            recovery_sheet.update(f"A1:G{len(leasing_data)}", leasing_data)
            print(f"✅ Leasing data copied ({len(leasing_data)} rows)")

        # Copy revshare data
        rev_data = revshare_sheet.get("A:H", value_render_option="UNFORMATTED_VALUE")
        if not rev_data:
            print("⚠️ No revshare data found.")
        else:
            filtered = [row for i, row in enumerate(rev_data) if i == 0 or row[0] != ""]
            final_data = [[r[0], r[1], r[3], r[4], r[5], r[6], r[7]] for r in filtered]
            start_row = len(leasing_data) + 3 if leasing_data else 2
            recovery_sheet.update(f"A{start_row}:G{start_row + len(final_data) - 1}", final_data)
            print(f"✅ Revshare data appended ({len(final_data)} rows)")

        print("🎯 Recovery sheet updated successfully!\n")
    except Exception as e:
        print(f"❌ updateRecovery failed: {e}")


# ===== CNG OS COLLECTION =====
def importCNGOSCollectionFast():
    print("\n▶️ Running importCNGOSCollectionFast...")
    try:
        SOURCE_SHEET_ID = "1sipU5ThP9PmJYBBn06XxGZkPvUNobBCHQWo8jNwUyuw"
        SOURCE_TAB = "OS_Collection"
        TARGET_SHEET_ID = "1HMlQzPbqpEh2OiIZT6h5UxjfY-wmWUrLQDgahNsxzl0"
        TARGET_TAB = "CNG_OS_Summary"

        client = get_gsheet_client()
        source = client.open_by_key(SOURCE_SHEET_ID).worksheet(SOURCE_TAB)
        target = client.open_by_key(TARGET_SHEET_ID).worksheet(TARGET_TAB)

        filter_date_str = target.acell("E1").value
        print(f"ℹ️ Filter date in E1: {filter_date_str}")
        if not filter_date_str:
            print("⚠️ No date in E1. Skipping importCNGOSCollectionFast.")
            return

        filter_date = datetime.strptime(filter_date_str, "%d/%m/%Y").date()
        data = source.get_all_values()
        if not data:
            print("⚠️ No source data found.")
            return

        output = []
        for r in data[1:]:
            r = r + [""] * 18
            try:
                if not r[0]:
                    continue
                row_date = datetime.strptime(r[0], "%d/%m/%Y").date()
                if row_date == filter_date and r[1].strip() != "Delhi NCR":
                    mapped = [r[3], r[0], r[1], r[2], r[5], r[4], r[10], r[16], r[17]]
                    output.append(mapped)
            except Exception as e:
                print(f"⚠️ Skipping row due to error: {e}")

        if not output:
            print("⚠️ No matching data found for filter date.")
            return

        last_row = len(target.get_all_values())
        target.batch_clear([f"E3:M{last_row}"])
        target.update("E3", output)

        print(f"✅ importCNGOSCollectionFast completed. Rows: {len(output)}")
    except Exception as e:
        print(f"❌ importCNGOSCollectionFast failed: {e}")


# ===== MAIN EXECUTION =====
if __name__ == "__main__":
    print("🚀 Starting All_collection_recovery.py...")
    ossummarycollection()
    updateRecovery()
    importCNGOSCollectionFast()
    print("✅ All tasks completed!")

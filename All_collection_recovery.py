import gspread
from google.oauth2.service_account import Credentials
import pandas as pd

# ------------- AUTH SETUP -----------------
SERVICE_ACCOUNT_FILE = "rare-sunrise-446516-u4-897494306ac4.json"

creds = Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE,
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)
client = gspread.authorize(creds)


# ------------- HELPER: Column Index to Letter -----------------
def col_number_to_letter(n):
    """Convert column number (1-based) to letter (e.g. 1 -> A, 27 -> AA)"""
    result = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result


# ------------- FUNCTION 1: OS Summary Collection -----------------
def ossummarycollection():
    print("▶️ Running ossummarycollection...")

    source_id = "1D4LjhxfaBpV1zUSCrQ7Xfe2NpeNRNgSdli16lh4anlo"
    target_id = "1sipU5ThP9PmJYBBn06XxGZkPvUNobBCHQWo8jNwUyuw"
    source_tab = "OS_ETM_Summary"
    target_tab = "OS_Collection"

    source_ws = client.open_by_key(source_id).worksheet(source_tab)
    target_ws = client.open_by_key(target_id).worksheet(target_tab)

    all_data = source_ws.get_all_values()
    if len(all_data) < 3:
        print("⚠️ Not enough rows in source.")
        return

    headers = all_data[2]  # A3:Q3
    data = all_data[3:]    # A4 onwards

    # Filter B == one of target cities
    filtered = [row for row in data if row[1] in ['Delhi NCR', 'Sukhrali', 'Noida', 'Delhi']]

    # Clear only A:Q
    target_ws.batch_clear(["A:Q"])

    # Update header and data
    last_col_letter = col_number_to_letter(len(headers))
    target_ws.update(f"A1:{last_col_letter}1", [headers])

    if filtered:
        target_ws.update(f"A2:{last_col_letter}{len(filtered)+1}", filtered)

    print(f"✅ OS Collection updated with {len(filtered)} rows.")


# ------------- FUNCTION 2: updateRecovery -----------------
def updateRecovery():
    print("▶️ Running updateRecovery...")

    source_id = "1D4LjhxfaBpV1zUSCrQ7Xfe2NpeNRNgSdli16lh4anlo"
    target_id = "1sipU5ThP9PmJYBBn06XxGZkPvUNobBCHQWo8jNwUyuw"

    leasing_tab = "Leasing_Raw"
    revshare_tab = "Revshare_Raw"
    target_tab = "Recovery"

    source = client.open_by_key(source_id)
    target = client.open_by_key(target_id)

    leasing_ws = source.worksheet(leasing_tab)
    revshare_ws = source.worksheet(revshare_tab)
    target_ws = target.worksheet(target_tab)

    # Clear A:G in target
    target_ws.batch_clear(["A:G"])

    # Part 1: Leasing
    leasing_data = leasing_ws.get_all_values()
    leasing_data = [row[:7] for row in leasing_data]  # First 7 columns
    target_ws.update(f"A1:G{len(leasing_data)}", leasing_data)

    # Part 2: Revshare
    rev_data = revshare_ws.get_all_values()
    filtered = [row for i, row in enumerate(rev_data) if i == 0 or row[0] != ""]
    final_data = [[r[0], r[1], r[3], r[4], r[5], r[6], r[7]] for r in filtered]

    start_row = len(leasing_data) + 3
    target_ws.update(f"A{start_row}:G{start_row + len(final_data) - 1}", final_data)

    print(f"✅ Recovery sheet updated: Leasing({len(leasing_data)}), Revshare({len(final_data)})")


# ------------- FUNCTION 3: importCNGOSCollectionFast -----------------
def importCNGOSCollectionFast():
    print("▶️ Running importCNGOSCollectionFast...")

    target_id = "1D4LjhxfaBpV1zUSCrQ7Xfe2NpeNRNgSdli16lh4anlo"
    source_id = "1sipU5ThP9PmJYBBn06XxGZkPvUNobBCHQWo8jNwUyuw"

    target_tab = "CNG_OS_Summary"
    source_tab = "OS_Collection"

    target_ws = client.open_by_key(target_id).worksheet(target_tab)
    source_ws = client.open_by_key(source_id).worksheet(source_tab)

    filter_date_str = target_ws.acell("E1").value
    if not filter_date_str:
        print("⚠️ No date in E1")
        return

    try:
        filter_date = pd.to_datetime(filter_date_str).normalize()
    except Exception as e:
        print(f"❌ Invalid date in E1: {e}")
        return

    data = source_ws.get_all_values()
    if not data:
        print("⚠️ No data in OS_Collection")
        return

    filtered = []
    for i, row in enumerate(data[1:], start=2):
        try:
            if row[0] and pd.to_datetime(row[0]).normalize() == filter_date and row[1] != "Delhi NCR":
                filtered.append([row[3], row[0], row[1], row[2], row[5], row[4], row[10], row[16], row[17]])
        except:
            continue

    # Clear E2:M (9 columns)
    target_ws.batch_clear(["E2:M"])

    if filtered:
        target_ws.update(f"E2:M{len(filtered)+1}", filtered)
        print(f"✅ importCNGOSCollectionFast: {len(filtered)} rows added.")
    else:
        print("⚠️ No matching data found for importCNGOSCollectionFast")


# ------------- MAIN -----------------
if __name__ == "__main__":
    print("All_collection_recovery.py...")

    try:
        ossummarycollection()
    except Exception as e:
        print(f"❌ Error in ossummarycollection: {e}")

    try:
        updateRecovery()
    except Exception as e:
        print(f"❌ Error in updateRecovery: {e}")

    try:
        importCNGOSCollectionFast()
    except Exception as e:
        print(f"❌ Error in importCNGOSCollectionFast: {e}")

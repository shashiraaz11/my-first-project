import gspread
from google.oauth2.service_account import Credentials
import pandas as pd

# ---------------- AUTH SETUP ----------------
SERVICE_ACCOUNT_FILE = "rare-sunrise-446516-u4-897494306ac4.json"

creds = Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE,
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)
client = gspread.authorize(creds)

# ---------------- OS SUMMARY COLLECTION ----------------
def ossummarycollection():
    print("▶️ Running ossummarycollection...")

    source_id = "1D4LjhxfaBpV1zUSCrQ7Xfe2NpeNRNgSdli16lh4anlo"
    target_id = "1sipU5ThP9PmJYBBn06XxGZkPvUNobBCHQWo8jNwUyuw"
    source_tab = "OS_ETM_Summary"
    target_tab = "OS_Collection"

    source = client.open_by_key(source_id).worksheet(source_tab)
    target = client.open_by_key(target_id).worksheet(target_tab)

    data = source.get_all_values()[2:]  # skip first two rows
    headers = source.row_values(3)

    filtered = [row for row in data if row[1] in ["Delhi NCR", "Sukhrali", "Noida", "Delhi"]]

    target.clear()
    target.update(values=[headers], range_name="A1:Q1")

    if filtered:
        target.update(values=filtered, range_name=f"A2:Q{len(filtered)+1}")

    print(f"✅ Filtered rows: {len(filtered)}")
    print("✅ OS Collection updated successfully!\n")

# ---------------- RECOVERY DATA UPDATE ----------------
def updateRecovery():
    print("▶️ Running updateRecovery...")

    source_id = "1D4LjhxfaBpV1zUSCrQ7Xfe2NpeNRNgSdli16lh4anlo"
    target_id = "1sipU5ThP9PmJYBBn06XxGZkPvUNobBCHQWo8jNwUyuw"

    leasing_tab = "Leasing_Raw"
    revshare_tab = "Revshare_Raw"
    recovery_tab = "Recovery"

    source = client.open_by_key(source_id)
    target = client.open_by_key(target_id)

    leasing = source.worksheet(leasing_tab)
    revshare = source.worksheet(revshare_tab)
    recovery_sheet = target.worksheet(recovery_tab)

    recovery_sheet.batch_clear(["A:G"])

    leasing_data = leasing.get_all_values()
    recovery_sheet.update(values=leasing_data, range_name=f"A1:G{len(leasing_data)}")
    print(f"✅ Leasing data copied ({len(leasing_data)} rows)")

    rev_data = revshare.get_all_values()
    filtered = [row for i, row in enumerate(rev_data) if i == 0 or row[0]]
    final_data = [[r[0], r[1], r[3], r[4], r[5], r[6], r[7]] for r in filtered]

    start_row = len(leasing_data) + 3
    recovery_sheet.update(values=final_data, range_name=f"A{start_row}:G{start_row + len(final_data) - 1}")

    print(f"✅ Revshare data appended ({len(final_data)} rows)")
    print("🎯 Recovery sheet updated successfully!\n")

# ---------------- CNG OS COLLECTION FAST ----------------
def importCNGOSCollectionFast(client):
    print("▶️ Running importCNGOSCollectionFast...")

    target_id = "1D4LjhxfaBpV1zUSCrQ7Xfe2NpeNRNgSdli16lh4anlo"
    source_id = "1sipU5ThP9PmJYBBn06XxGZkPvUNobBCHQWo8jNwUyuw"

    target_tab = "CNG_OS_Summary"
    source_tab = "OS_Collection"

    target = client.open_by_key(target_id).worksheet(target_tab)
    source = client.open_by_key(source_id).worksheet(source_tab)

    filter_date_str = target.acell("E1").value
    if not filter_date_str:
        print("⚠️ No date found in E1")
        return

    filter_date = pd.to_datetime(filter_date_str).normalize()

    data = source.get_all_values()
    header = data[0]
    filtered = [header]

    for i, row in enumerate(data[1:], start=2):
        try:
            if row[0]:
                date_val = pd.to_datetime(row[0]).normalize()
                if date_val == filter_date and row[1] != "Delhi NCR":
                    filtered.append([row[3], row[0], row[1], row[2], row[5], row[4], row[10], row[16], row[17]])
        except Exception:
            continue

    if len(filtered) == 1:
        print("⚠️ No matching data found")
    else:
        target.batch_clear(["E2:M"])
        target.update(values=filtered, range_name=f"E2:M{len(filtered)+1}")

    print(f"✅ importCNGOSCollectionFast completed. Rows: {len(filtered)-1}\n")

# ---------------- MAIN ----------------
if __name__ == "__main__":
    print("🚀 Starting All_collection_recovery.py...")
    ossummarycollection()
    updateRecovery()
    importCNGOSCollectionFast(client)

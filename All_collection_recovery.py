import gspread
import pandas as pd
from google.oauth2.service_account import Credentials

def ossummarycollection():
    print("▶️ Running ossummarycollection...")

    # Google Sheet IDs
    source_id = "1D4LjhxfaBpV1zUSCrQ7Xfe2NpeNRNgSdli16lh4anlo"
    target_id = "1sipU5ThP9PmJYBBn06XxGZkPvUNobBCHQWo8jNwUyuw"

    # Sheet names
    source_tab = "OS_ETM_Summary"
    target_tab = "OS_Collection"

    # Auth setup
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(
        "rare-sunrise-446516-u4-897494306ac4.json",
        scopes=SCOPES
    )
    client = gspread.authorize(creds)

    # Open sheets
    source = client.open_by_key(source_id).worksheet(source_tab)
    target = client.open_by_key(target_id).worksheet(target_tab)

    # Get data A:Q only
    data = source.get("A:Q", value_render_option="UNFORMATTED_VALUE")

    if len(data) <= 2:
        print("⚠️ No data found to copy.")
        return

    headers = data[2]  # assuming header is in row 3
    rows = data[3:]    # from row 4 onwards

    # Filter rows where column B matches (case-insensitive)
    filtered = [
        row for row in rows
        if len(row) > 1 and str(row[1]).strip().lower() in ["delhi ncr", "sukhrali", "noida", "delhi"]
    ]

    print(f"✅ Filtered rows: {len(filtered)}")

    # Clear target A:Q
    target.batch_clear(["A:Q"])

    # Write header (1st row)
    target.update(range_name="A1:Q1", values=[headers])

    # Write filtered data below
    if filtered:
        target.update(
            range_name=f"A2:Q{len(filtered) + 1}",
            values=filtered
        )

    print("✅ OS Summary copied successfully!")

if __name__ == "__main__":
    ossummarycollection()

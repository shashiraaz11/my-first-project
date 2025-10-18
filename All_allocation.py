import os
import json
import time
import traceback
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

# ===========================
# 🔐 Google Sheets Auth Setup
# ===========================
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

SERVICE_ACCOUNT_FILE = r"C:\Users\USER\Desktop\google_sheets_project\crediantnal_keye.json"
key_data = os.environ.get("ACCOUNT_KEY_JSON")

if key_data:
    creds_dict = json.loads(key_data)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    print("✅ Using credentials from environment variable.")
else:
    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        raise FileNotFoundError("❌ No ACCOUNT_KEY_JSON env variable or local file found.")
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    print("✅ Using local service account file.")

client = gspread.authorize(creds)

# ===========================
# 📊 Sheet Configurations
# ===========================
DEST_SHEET_URL = "https://docs.google.com/spreadsheets/d/1HGkBcL4mxgrTs5wNhWz9OgALIE-2pMoZ_P8P2vCsIls/edit?gid=1842610227#gid=1842610227"
DEST_SPREADSHEET_ID = "1HGkBcL4mxgrTs5wNhWz9OgALIE-2pMoZ_P8P2vCsIls"

SOURCES = [
    {
        "url": "https://docs.google.com/spreadsheets/d/1z1EsD9S4yIjn3MNAAjvdsApw3T6R-K8KuJf2s1rnkgE/edit",
        "sheet_name": "raw_leads",
        "columns": "A:AC",
        "destination": "FSE"
    },
    {
        "url": "https://docs.google.com/spreadsheets/d/1z1EsD9S4yIjn3MNAAjvdsApw3T6R-K8KuJf2s1rnkgE/edit",
        "sheet_name": "Exception_File",
        "columns": "A:S",
        "destination": "Exception_File"
    },
    {
        "url": "https://docs.google.com/spreadsheets/d/1AYOZnHLQBi7GIqn1PdTNNRko5gwtngJZZhB_UCHPQPo/edit",
        "sheet_name": "raw_leads",
        "columns": "A:AC",
        "destination": "Vendor"
    },
    {
        "url": "https://docs.google.com/spreadsheets/d/1fcHmuexLkj_Rjxai38aSewk76lcls-O9EDR1VGPul5w/edit",
        "sheet_name": "New Joins",
        "columns": "A:Q",
        "destination": "Telecalling"
    },
    {
        "url": "https://docs.google.com/spreadsheets/d/1MHG10SDEYoeBfq512t_rkBb7lpUvBD3XNRCLUrv0MCc/edit",
        "sheet_name": "Raw_Data",
        "columns": "A:AC",
        "destination": "Referal",
        "filter_column": 3  # zero-based index (D column)
    },
    {
        "url": "https://docs.google.com/spreadsheets/d/1jmqNQt1VIKCAFCg9qBhkeYr1zrNg54Q-9c1PHY_bb5w/edit",
        "sheet_name": "Rejoinings",
        "columns": "A:P",
        "destination": "Rejoin"
    }
]

# ===========================
# 📥 Functions
# ===========================

def get_data_from_source(client, url, sheet_name, columns, filter_column=2):
    """Fetch and filter data from source sheet (Delhi NCR)."""
    try:
        print(f"📖 Reading: {sheet_name} ({url})")
        sheet = client.open_by_url(url).worksheet(sheet_name)
        data = sheet.get(columns, value_render_option='UNFORMATTED_VALUE')

        df = pd.DataFrame(data)
        if df.empty or len(df.columns) <= filter_column:
            print("⚠️ No usable data or filter column out of range.")
            return pd.DataFrame(), []

        headers = df.iloc[0].tolist()
        df = df.iloc[1:]  # Remove header row

        # Filter by 'Delhi NCR' (case-insensitive)
        df_filtered = df[df.iloc[:, filter_column].astype(str).str.strip().str.lower() == "delhi ncr"]
        print(f"✅ Rows after filter: {len(df_filtered)}")
        return df_filtered, headers

    except Exception:
        print("❌ Error while fetching data:")
        traceback.print_exc()
        return pd.DataFrame(), []


def update_destination(client, destination_url, sources):
    """Merge data from all sources into destination."""
    try:
        print("📂 Opening destination spreadsheet...")
        dest_sheet = client.open_by_url(destination_url)

        for source in sources:
            print(f"\n🔄 Processing: {source['destination']}")
            df_filtered, headers = get_data_from_source(
                client,
                source["url"],
                source["sheet_name"],
                source["columns"],
                source.get("filter_column", 2)
            )

            if df_filtered.empty:
                print(f"⚠️ No data found for {source['destination']}")
                continue

            # Get or create worksheet
            try:
                dest_worksheet = dest_sheet.worksheet(source["destination"])
            except gspread.exceptions.WorksheetNotFound:
                print(f"🆕 Creating new sheet: {source['destination']}")
                dest_worksheet = dest_sheet.add_worksheet(title=source["destination"], rows="1000", cols=str(len(headers)))

            # Clear old data
            print("🧹 Clearing old data...")
            dest_worksheet.clear()

            # Write new data
            print(f"📝 Writing {len(df_filtered)} rows...")
            dest_worksheet.append_row(headers)
            dest_worksheet.append_rows(df_filtered.values.tolist())

            print(f"✅ Updated: {source['destination']}")
            time.sleep(2)  # avoid rate limit

    except Exception:
        print("❌ Error while updating destination:")
        traceback.print_exc()


def import_new_joining(client):
    """Special import for New Joining tab with Column U preservation."""
    try:
        print("\n📋 Importing New Joining data...")
        source_spreadsheet_id = "1o6nrw8zgg48q1Qbn01J23M8ePYel9IraqXCcuUPMwlM"
        destination_sheet_name = "New Joining"

        source_ss = client.open_by_key(source_spreadsheet_id)
        source_sheet = source_ss.worksheet("Raw Data")

        source_data = source_sheet.get_values(value_render_option='UNFORMATTED_VALUE')
        if not source_data:
            print("⚠️ No data found in source sheet.")
            return

        headers = source_data[0]
        filtered_rows = [row for row in source_data[1:] if row and row[0].strip().lower() == "delhi ncr"]
        if not filtered_rows:
            print("⚠️ No matching data found (Delhi NCR).")
            return

        filtered_data = [headers] + filtered_rows

        destination_ss = client.open_by_key(DEST_SPREADSHEET_ID)
        destination_sheet = destination_ss.worksheet(destination_sheet_name)

        existing_column_u = destination_sheet.col_values(21)
        destination_sheet.batch_clear(["A1:U"])

        new_data = []
        for i, row in enumerate(filtered_data):
            row = row[:21] + [""] * (21 - len(row))
            if i < len(existing_column_u):
                row[20] = existing_column_u[i]
            new_data.append(row)

        destination_sheet.update(f"A1:U{len(new_data)}", new_data)
        destination_sheet.format("A1:U1", {
            "backgroundColor": {"red": 0.26, "green": 0.52, "blue": 0.96},
            "textFormat": {"foregroundColor": {"red": 1, "green": 1, "blue": 1}, "bold": True}
        })

        print(f"✅ {len(filtered_data) - 1} rows imported successfully to '{destination_sheet_name}'.")

    except Exception:
        print("❌ Error in import_new_joining:")
        traceback.print_exc()


# ===========================
# 🏁 Main Entry Point
# ===========================
def main():
    print("\n🚀 Script started...")
    try:
        update_destination(client, DEST_SHEET_URL, SOURCES)
        import_new_joining(client)
        print("\n🎉 Script finished successfully!")
    except Exception:
        print("💥 Fatal error during execution:")
        traceback.print_exc()


if __name__ == "__main__":
    main()

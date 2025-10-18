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

key_data = os.environ.get("ACCOUNT_KEY_JSON")
if key_data:
    creds_dict = json.loads(key_data)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    print("✅ Using credentials from environment variable.")
else:
    SERVICE_ACCOUNT_FILE = r"C:\Users\skuma\OneDrive\Desktop\Python\rare-sunrise-446516-u4-897494306ac4.json"
    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        raise FileNotFoundError("❌ No ACCOUNT_KEY_JSON env variable or local file found.")
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    print("✅ Using local service account file.")

client = gspread.authorize(creds)

# ===========================
# 🔗 Sheet Configuration
# ===========================
DEST_SHEET_URL = "https://docs.google.com/spreadsheets/d/1HGkBcL4mxgrTs5wNhWz9OgALIE-2pMoZ_P8P2vCsIls/edit"
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
        "filter_column": 3
    },
    {
        "url": "https://docs.google.com/spreadsheets/d/1jmqNQt1VIKCAFCg9qBhkeYr1zrNg54Q-9c1PHY_bb5w/edit",
        "sheet_name": "Rejoinings",
        "columns": "A:P",
        "destination": "Rejoin"
    }
]


def get_data_from_source(client, url, sheet_name, columns, filter_column=2):
    """Fetch data from a source Google Sheet and filter for 'Delhi NCR'."""
    try:
        print(f"📄 Reading data from: {sheet_name} | {url}")
        sheet = client.open_by_url(url).worksheet(sheet_name)
        data = sheet.get(columns, value_render_option='UNFORMATTED_VALUE')

        df = pd.DataFrame(data)
        if df.empty or len(df.columns) <= filter_column:
            print("⚠️ No usable data or filter column out of range.")
            return pd.DataFrame(), []

        headers = df.iloc[0].tolist()
        df = df.iloc[1:]  # remove header

        df_filtered = df[df.iloc[:, filter_column].astype(str).str.strip().str.lower() == "delhi ncr"]
        print(f"✅ Rows after filter: {len(df_filtered)}")
        return df_filtered, headers

    except Exception:
        print("❌ Error while fetching data:")
        traceback.print_exc()
        return pd.DataFrame(), []


def update_destination(client, destination_url, sources):
    """Merge data into the destination Google Sheet."""
    try:
        print("📂 Opening destination spreadsheet...")
        dest_sheet = client.open_by_url(destination_url)

        for source in sources:
            print(f"\n🚀 Processing source: {source['destination']}")
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

            try:
                dest_worksheet = dest_sheet.worksheet(source["destination"])
            except gspread.exceptions.WorksheetNotFound:
                print(f"🆕 Creating new sheet: {source['destination']}")
                dest_worksheet = dest_sheet.add_worksheet(title=source["destination"], rows="1000", cols=str(len(headers)))

            print("🧹 Clearing old data...")
            dest_worksheet.clear()

            print(f"✏️ Writing headers and {len(df_filtered)} rows...")
            dest_worksheet.append_row(headers)
            dest_worksheet.append_rows(df_filtered.values.tolist())

            print(f"✅ Updated: {source['destination']}")

    except Exception:
        print("❌ Error while updating destination:")
        traceback.print_exc()


def main():
    print("🔥 Script started...")
    update_destination(client, DEST_SHEET_URL, SOURCES)
    print("🎉 Script finished successfully!")


if __name__ == "__main__":
    main()

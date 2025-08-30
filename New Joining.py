import gspread
from google.oauth2.service_account import Credentials
import os
import json
import tempfile

def import_data():
    # Spreadsheet details
    source_spreadsheet_id = "1o6nrw8zgg48q1Qbn01J23M8ePYel9IraqXCcuUPMwlM"
    destination_spreadsheet_id = "1HGkBcL4mxgrTs5wNhWz9OgALIE-2pMoZ_P8P2vCsIls"
    destination_sheet_name = "New Joining"

    # Load service account info from environment variable
    service_account_info = json.loads(os.environ["MY_API_KEY"])

    # Create temporary JSON file from secret
    with tempfile.NamedTemporaryFile(mode='w+', suffix='.json', delete=False) as temp_file:
        json.dump(service_account_info, temp_file)
        temp_file_path = temp_file.name

    try:
        # Authenticate using the service account
        creds = Credentials.from_service_account_file(
            temp_file_path,
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        client = gspread.authorize(creds)

        # --- (rest of your code remains the same) ---

        # Open source sheet
        source_ss = client.open_by_key(source_spreadsheet_id)
        source_sheet = source_ss.worksheet("Raw Data")

        # Get data from source sheet
        source_data = source_sheet.get_values(value_render_option='UNFORMATTED_VALUE')
        if not source_data:
            print("No data found in source sheet.")
            return

        headers = source_data[0]

        # Filter rows where the first column is 'Delhi NCR'
        filtered_rows = [
            row for row in source_data[1:]
            if row and row[0].strip().lower() == "delhi ncr"
        ]

        if not filtered_rows:
            print("No matching data found (Delhi NCR).")
            return

        filtered_data = [headers] + filtered_rows

        # Open destination sheet
        destination_ss = client.open_by_key(destination_spreadsheet_id)
        destination_sheet = destination_ss.worksheet(destination_sheet_name)

        # Read existing data in column U
        existing_column_u = destination_sheet.col_values(21)

        # Clear columns A to U
        destination_sheet.batch_clear(["A1:U"])

        # Prepare new data
        new_data = []
        for i, row in enumerate(filtered_data):
            row = row[:21] + [""] * (21 - len(row))
            if i < len(existing_column_u):
                row[20] = existing_column_u[i]
            new_data.append(row)

        # Update destination
        destination_sheet.update(f"A1:U{len(new_data)}", new_data)

        # Format headers
        destination_sheet.format("A1:U1", {
            "backgroundColor": {"red": 0.26, "green": 0.52, "blue": 0.96},
            "textFormat": {"foregroundColor": {"red": 1, "green": 1, "blue": 1}, "bold": True}
        })

        print(f"{len(filtered_data) - 1} rows imported successfully to '{destination_sheet_name}'.")

    finally:
        os.remove(temp_file_path)  # Clean up temp file

if __name__ == "__main__":
    import_data()

import gspread
from google.oauth2.service_account import Credentials

def import_data():
    # Spreadsheet details
    source_spreadsheet_id = "1o6nrw8zgg48q1Qbn01J23M8ePYel9IraqXCcuUPMwlM"
    destination_spreadsheet_id = "1HGkBcL4mxgrTs5wNhWz9OgALIE-2pMoZ_P8P2vCsIls"
    destination_sheet_name = "New Joining"
    service_account_file = r"C:\Users\skuma\OneDrive\Desktop\Python\rare-sunrise-446516-u4-897494306ac4.json"
    
    # Authenticate using the service account
    creds = Credentials.from_service_account_file(  
        service_account_file,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    client = gspread.authorize(creds)
    
    # Open source sheet
    source_ss = client.open_by_key(source_spreadsheet_id)
    source_sheet = source_ss.worksheet("Raw Data")
    
    # Get data from source sheet
    source_data = source_sheet.get_values(value_render_option='UNFORMATTED_VALUE')
    if not source_data:
        print("No data found in source sheet.")
        return
    
    headers = source_data[0]
    
    # Filter rows where the first column is 'Delhi NCR' (case-insensitive)
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
    
    # Read existing data in column U (21st column)
    existing_column_u = destination_sheet.col_values(21)
    
    # Clear only columns A to U
    destination_sheet.batch_clear(["A1:U"])
    
    # Prepare new data, preserving existing column U values
    new_data = []
    for i, row in enumerate(filtered_data):
        row = row[:21] + [""] * (21 - len(row))  # Ensure row has exactly 21 columns
        if i < len(existing_column_u):
            row[20] = existing_column_u[i]  # Preserve existing value in column U
        new_data.append(row)
    
    # Update destination with filtered data (A1:U)
    destination_sheet.update(f"A1:U{len(new_data)}", new_data)
    
    # Format headers
    destination_sheet.format("A1:U1", {
        "backgroundColor": {"red": 0.26, "green": 0.52, "blue": 0.96},
        "textFormat": {"foregroundColor": {"red": 1, "green": 1, "blue": 1}, "bold": True}
    })
    
    print(f"{len(filtered_data) - 1} rows imported successfully to '{destination_sheet_name}'.")

if __name__ == "__main__":
    import_data()

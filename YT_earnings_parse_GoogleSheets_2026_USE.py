#!/usr/bin/env python3

"""
YT Earnings Parsing Script - Google Sheets Version (Actuals Sheet)
==================================================================

This version writes consolidated earnings data directly to a Google Sheet
instead of creating a CSV file. Data is written to tabs named in "mmm yy" format.

Changes from USE version:
 1. Writes to different Google Sheet (Actuals sheet)
 2. Payout values are written as negative numbers

"""

import os
import re
import io
import sys
import logging
import subprocess
from datetime import datetime
import calendar

import pandas as pd
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

#############################################################################
# Configuration
#############################################################################

# Google Sheet ID (extracted from the URL)
SHEET_ID = "1ZGfvrUcQspv30WugvMF-jGHKvl9KpQdWFP6bBfEX7ik"

# Column map for recognized specifiers to the numeric revenue column.
REVENUE_COLUMN_MAP = {
    "Subscription Revenue Video Report": "Partner Revenue",
    "Subscription Revenue Report": "Partner Revenue",
    "Paid Features Report": "Earnings (USD)",
    "Non Music Video Summary Premium": "Partner Revenue",
    "Shorts Ads Revenue": "Net Partner Revenue (Post revshare)",
    "Ads Revenue Video Summary": "Partner Revenue",
    "Ads Adjustment Report": "Partner Revenue",
    "Shorts Subscription Revenue": "Partner Revenue"
}

# List of filename prefixes requiring the first row to be skipped.
SKIP_FIRST_ROW_PREFIXES = [
    # "Subscription Revenue Video Report",  # Provide examples if needed.
    "Paid Features Report",
    "Shorts Subscription Revenue Video Summary",
    "Premium Non Music Asset Video Summary"
]

#############################################################################
# Step 1) Prompt for Google Drive folder URL
#############################################################################
def ask_for_folder_url() -> str:
    """
    Attempt to prompt the user with AppleScript on macOS. If not available,
    fall back to a raw input prompt.
    """
    if sys.platform == 'darwin':
        try:
            # Attempt AppleScript
            script = 'display dialog "Enter the Google Drive folder URL:" default answer ""'
            proc = subprocess.Popen(["osascript", "-e", script], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            output, _ = proc.communicate()
            output = output.decode("utf-8").strip()
            # The output is like: "button returned:OK, text returned:YOUR_URL"
            for part in output.split(","):
                if "text returned:" in part:
                    return part.split("text returned:")[1].strip()
        except Exception as e:
            logging.warning(f"AppleScript prompting failed: {e}")
            pass

    # If not on macOS or AppleScript fails, use standard input.
    folder_url = input("Enter the Google Drive folder URL: ")
    return folder_url.strip()

#############################################################################
# Step 2) Parse a folder ID from the URL
#############################################################################
def parse_folder_id(folder_url: str) -> str:
    match = re.search(r'/folders/([a-zA-Z0-9_-]+)', folder_url)
    if match:
        return match.group(1)
    logging.error("Could not parse a folder ID from the URL.")
    sys.exit(1)

#############################################################################
# Step 3) Parse filename to extract date and specifier
#############################################################################
def parse_filename(file_name: str):
    # Remove .csv extension.
    if file_name.lower().endswith('.csv'):
        file_name = file_name[:-4].strip()

    parts = file_name.split()
    # Assume the last part is the date portion, e.g. "1-2025".
    date_str = parts[-1]
    raw_specifier = " ".join(parts[:-1])

    # Simplify / standardize specifier.
    if raw_specifier.startswith("Shorts Ads Revenue"):
        specifier = "Shorts Ads Revenue"
    elif raw_specifier.startswith("Ads Adjustment"):
        specifier = "Ads Adjustment Report"
    elif raw_specifier.startswith("Shorts Subscription Revenue"):
        specifier = "Shorts Subscription Revenue"
    elif raw_specifier.startswith("Premium Non Music Asset"):
        specifier = "Non Music Video Summary Premium"
    else:
        specifier = raw_specifier

    # Convert date_str from e.g. "1-2025" to a standardized format YYYY-MM.
    # We'll attempt to parse the month-year as integer.
    standardized_date_str = date_str
    try:
        # Expect something like "1-2025" => month=1, year=2025
        month_year = date_str.split("-")
        if len(month_year) == 2:
            month = int(month_year[0])
            year = int(month_year[1])
            # Convert to YYYY-MM for consistent usage.
            standardized_date_str = f"{year:04d}-{month:02d}"
        else:
            logging.warning(f"Unexpected date format '{date_str}'. Keeping as-is.")
    except ValueError:
        logging.warning(f"Could not parse date string '{date_str}'. Keeping as-is.")

    return standardized_date_str, specifier

#############################################################################
# Step 4) CSV reading with error handling
#############################################################################
def try_read_csv(file_content: str, force_skip=False, skip_first_row_if_needed=True) -> pd.DataFrame:
    """
    Attempt to read CSV using different skip-row strategies.
    Returns a DataFrame if success, or None if completely failed.
    """
    try:
        if force_skip:
            df = pd.read_csv(io.StringIO(file_content), skiprows=1)
            df.columns = [c.strip() for c in df.columns]
            return df
        else:
            # First, try with no skipping.
            df = pd.read_csv(io.StringIO(file_content))
            df.columns = [c.strip() for c in df.columns]
            if "Channel ID" in df.columns:
                return df
            # If not found, try skipping 1 row.
            if skip_first_row_if_needed:
                df2 = pd.read_csv(io.StringIO(file_content), skiprows=1)
                df2.columns = [c.strip() for c in df2.columns]
                if "Channel ID" in df2.columns:
                    return df2
            # If still not found, fail.
            return None
    except Exception as e:
        logging.warning(f"Error reading CSV: {e}")
        return None

#############################################################################
# Step 5) Aggregate channel data from a DataFrame
#############################################################################
def extract_channel_data(df: pd.DataFrame) -> pd.DataFrame:
    if 'Channel ID' in df.columns and 'Channel Display Name' in df.columns:
        return df[['Channel ID', 'Channel Display Name']]
    return pd.DataFrame(columns=['Channel ID', 'Channel Display Name'])

#############################################################################
# Step 6) Google Sheets helper functions
#############################################################################
def get_month_tab_name(date_str: str) -> str:
    """
    Convert date string (YYYY-MM) to tab name format (mmm yy).
    Example: "2025-01" -> "Jan 25"
    """
    try:
        year, month = date_str.split("-")
        month_num = int(month)
        year_short = year[-2:]
        month_abbr = calendar.month_abbr[month_num]
        return f"{month_abbr} {year_short}"
    except Exception as e:
        logging.error(f"Error converting date '{date_str}' to tab name: {e}")
        return None

def get_sheets_service(gauth):
    """
    Create Google Sheets service using existing PyDrive authentication.
    """
    # Get the credentials from PyDrive's GoogleAuth
    creds = Credentials(
        token=gauth.credentials.access_token,
        refresh_token=gauth.credentials.refresh_token,
        token_uri=gauth.credentials.token_uri,
        client_id=gauth.credentials.client_id,
        client_secret=gauth.credentials.client_secret
    )

    service = build('sheets', 'v4', credentials=creds)
    return service

def ensure_sheet_exists(service, sheet_id, sheet_name):
    """
    Check if a sheet tab exists, create it if it doesn't.
    """
    try:
        # Get all sheets
        sheet_metadata = service.spreadsheets().get(spreadsheetId=sheet_id).execute()
        sheets = sheet_metadata.get('sheets', [])

        # Check if sheet exists
        for sheet in sheets:
            if sheet['properties']['title'] == sheet_name:
                logging.info(f"Sheet '{sheet_name}' already exists.")
                return sheet['properties']['sheetId']

        # Create new sheet if it doesn't exist
        request_body = {
            'requests': [{
                'addSheet': {
                    'properties': {
                        'title': sheet_name
                    }
                }
            }]
        }

        response = service.spreadsheets().batchUpdate(
            spreadsheetId=sheet_id,
            body=request_body
        ).execute()

        new_sheet_id = response['replies'][0]['addSheet']['properties']['sheetId']
        logging.info(f"Created new sheet '{sheet_name}'.")
        return new_sheet_id

    except Exception as e:
        logging.error(f"Error ensuring sheet exists: {e}")
        raise

def clear_sheet(service, sheet_id, sheet_name):
    """
    Clear all content from a sheet.
    """
    try:
        range_name = f"'{sheet_name}'!A:Z"
        service.spreadsheets().values().clear(
            spreadsheetId=sheet_id,
            range=range_name
        ).execute()
        logging.info(f"Cleared sheet '{sheet_name}'.")
    except Exception as e:
        logging.error(f"Error clearing sheet: {e}")
        raise

def write_to_sheet(service, sheet_id, sheet_name, dataframe):
    """
    Write a DataFrame to a Google Sheet with formulas for Job Code and Current Split Lookup.
    Also adds expense rows calculated from revenue * split percentage.
    Payout values are written as negative numbers.
    """
    try:
        # First, write the headers
        headers = dataframe.columns.tolist()
        header_range = f"'{sheet_name}'!A1"
        header_body = {
            'values': [headers]
        }

        service.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range=header_range,
            valueInputOption='RAW',
            body=header_body
        ).execute()

        # Prepare data rows with formulas
        data_rows = []
        revenue_count = len(dataframe)

        # First, add all revenue rows
        for idx, row in dataframe.iterrows():
            row_num = idx + 2  # Excel rows start at 1, and we have headers at row 1

            # Create the revenue row data
            row_data = [
                row['State'],
                f"=XLOOKUP(D{row_num},'Payout Split Detail'!$A$2:$A$1000,'Payout Split Detail'!$B$2:$B$1000)",  # Job Code formula
                row['Channel ID'],
                row['Channel Display Name'],
                row['1 - Category'],
                'Revenue - Adrev',
                row['Actual Date'],
                '4110-Advertising Fee Income',
                row['3 - Specifier'],
                row['Value'],
                f"=IF(H{row_num}=\"Snap\",XLOOKUP(B{row_num},'Payout Split Detail'!$B$2:$B,'Payout Split Detail'!$F$2:$F),IF(H{row_num}=\"FB\",XLOOKUP(B{row_num},'Payout Split Detail'!$B$2:$B,'Payout Split Detail'!$E$2:$E),XLOOKUP(B{row_num},'Payout Split Detail'!$B$2:$B,'Payout Split Detail'!$D$2:$D)))",  # Current Split Lookup formula
            ]
            data_rows.append(row_data)

        # Then, add corresponding expense rows with negative payout values
        for idx, row in dataframe.iterrows():
            revenue_row_num = idx + 2  # Row number for corresponding revenue row

            # Create the expense row data
            expense_row_data = [
                row['State'],  # Same as revenue
                f'=B{revenue_row_num}',  # Copy Job Code from revenue row
                row['Channel ID'],  # Same as revenue
                row['Channel Display Name'],  # Same as revenue
                'Expense',  # Changed from Revenue
                'Payout - Ad Revenue ' + row['2 - Subcategory'],  # Add "Payout - Ad Revenue" prefix
                row['Actual Date'],  # Same as revenue
                '4520-Channel Partner Payouts',  # Same as revenue
                row['3 - Specifier'],  # Same as revenue
                f'=-1*(J{revenue_row_num}*K{revenue_row_num})',  # Negative of (Value * Split percentage)
                f'=K{revenue_row_num}'  # Copy split percentage from revenue row
            ]
            data_rows.append(expense_row_data)

        # Write data with formulas
        if data_rows:
            data_range = f"'{sheet_name}'!A2"
            data_body = {
                'values': data_rows
            }

            result = service.spreadsheets().values().update(
                spreadsheetId=sheet_id,
                range=data_range,
                valueInputOption='USER_ENTERED',  # This allows formulas to be interpreted
                body=data_body
            ).execute()

            logging.info(f"Wrote {result.get('updatedRows', 0)} rows to sheet '{sheet_name}'.")

    except Exception as e:
        logging.error(f"Error writing to sheet: {e}")
        raise

#############################################################################
# Step 7) Main function
#############################################################################
def main():
    # Option to use test folder or prompt for URL
    test_mode = len(sys.argv) > 1 and sys.argv[1] == "--test"

    if test_mode:
        folder_id = "1o-YoBQ2IP6KuA896EE41Su52c0-JvAmy"
        logging.info("Using test folder ID: " + folder_id)
    else:
        # Prompt user for folder URL.
        folder_url = ask_for_folder_url()
        # Parse folder ID.
        folder_id = parse_folder_id(folder_url)
        logging.info(f"Using folder ID: {folder_id}")

    # Authenticate with Google.
    gauth = GoogleAuth()
    gauth.LocalWebserverAuth()  # Opens or uses stored credentials
    drive = GoogleDrive(gauth)

    # Get Sheets service
    sheets_service = get_sheets_service(gauth)

    # Prepare a DataFrame to store unique channel data.
    all_channel_data = pd.DataFrame(columns=['Channel ID', 'Channel Display Name'])

    # List all CSV files in target folder.
    file_list = drive.ListFile({
        'q': f"'{folder_id}' in parents and mimeType='text/csv'"
    }).GetList()

    # We'll store aggregated rows by month.
    data_by_month = {}

    for f in file_list:
        file_name = f['title']
        # Skip final consolidated file if present.
        if file_name.startswith("Consolidated Earnings Sheet"):
            continue

        # Determine if we force skipping first row.
        force_skip = any(file_name.startswith(prefix) for prefix in SKIP_FIRST_ROW_PREFIXES)
        # Parse date & specifier.
        date_str, specifier = parse_filename(file_name)
        # Lookup revenue column.
        revenue_col = REVENUE_COLUMN_MAP.get(specifier)
        if not revenue_col:
            logging.warning(f"No revenue column found for '{file_name}' (specifier='{specifier}'). Skipping.")
            continue

        # Download content.
        downloaded = f.GetContentString()
        # Try reading CSV.
        df = try_read_csv(downloaded, force_skip=force_skip, skip_first_row_if_needed=True)
        if df is None:
            logging.warning(f"Could not parse '{file_name}' even with skip-rows strategies. Skipping.")
            continue

        # Clean up column names.
        df.columns = [c.strip() for c in df.columns]

        # Extract channel data.
        channel_subset = extract_channel_data(df)
        all_channel_data = (
            pd.concat([all_channel_data, channel_subset], ignore_index=True)
              .drop_duplicates()
        )

        if "Channel ID" not in df.columns:
            logging.warning(f"'Channel ID' column not found in '{file_name}'. Skipping.")
            continue

        # Group data.
        if "Channel" in df.columns:
            grouped_df = (
                df.groupby(["Channel ID", "Channel"], dropna=False, as_index=False)[revenue_col]
                  .sum()
            )
            grouped_df.rename(
                columns={
                    "Channel": "Channel Display Name",
                    revenue_col: "Value"
                },
                inplace=True
            )
        else:
            grouped_df = (
                df.groupby("Channel ID", dropna=False, as_index=False)[revenue_col]
                  .sum()
                  .rename(columns={revenue_col: "Value"})
            )
            grouped_df["Channel Display Name"] = None

        # Get month key for grouping
        month_key = date_str  # YYYY-MM format

        if month_key not in data_by_month:
            data_by_month[month_key] = []

        # Append each row to aggregated list for this month.
        for _, row in grouped_df.iterrows():
            data_by_month[month_key].append({
                "date_str": date_str,
                "specifier": specifier,
                "Channel ID": row["Channel ID"],
                "Channel Display Name": row["Channel Display Name"] if pd.notna(row["Channel Display Name"]) else None,
                "Value": row["Value"],
                "revenue_col": revenue_col
            })

    # Build a final Channel ID -> Display Name lookup.
    if not all_channel_data.empty:
        channel_mapping = (
            all_channel_data.groupby('Channel Display Name')['Channel ID']
            .apply(list)
            .reset_index()
        )
        expanded_channel_mapping = (
            channel_mapping
            .set_index('Channel Display Name')['Channel ID']
            .apply(pd.Series)
            .reset_index()
        )
        expanded_channel_mapping.columns = (
            ['Channel Display Name'] + [f'Channel ID {i+1}' for i in range(expanded_channel_mapping.shape[1] - 1)]
        )

        final_lookup = {}
        for _, row in expanded_channel_mapping.iterrows():
            display_name = row["Channel Display Name"]
            for col in row.index:
                if col.startswith("Channel ID"):
                    ch_id = row[col]
                    if pd.notna(ch_id) and str(ch_id).strip():
                        final_lookup[str(ch_id).strip()] = display_name
    else:
        final_lookup = {}

    # Process and write data for each month
    for month_key, month_data in data_by_month.items():
        # Create final DataFrame for this month
        month_df = pd.DataFrame(month_data)
        if month_df.empty:
            logging.warning(f"No data for month {month_key}. Skipping.")
            continue

        # Fill missing Channel Display Names.
        def fill_display_name(row):
            if pd.notna(row["Channel Display Name"]) and row["Channel Display Name"]:
                return row["Channel Display Name"]
            else:
                return final_lookup.get(str(row["Channel ID"]), str(row["Channel ID"]))

        month_df["Channel Display Name"] = month_df.apply(fill_display_name, axis=1)

        # Additional columns.
        month_df["State"] = "Actual"
        month_df["1 - Category"] = "Revenue"
        month_df["2 - Subcategory"] = "Ad Revenue"
        month_df["Actual Date"] = month_df["date_str"]
        month_df["3 - Specifier"] = month_df["specifier"]
        month_df["4 - Detail"] = month_df["revenue_col"]

        # Add Job Code column (will be populated with formulas later)
        month_df["Job Code"] = ""

        month_df = month_df[
            [
                "State",
                "Job Code",
                "Channel ID",
                "Channel Display Name",
                "1 - Category",
                "2 - Subcategory",
                "Actual Date",
                "3 - Specifier",
                "4 - Detail",
                "Value"
            ]
        ].copy()

        # Add Current Split Lookup column (will be populated with formulas later)
        month_df["Current Split Lookup"] = ""

        # Get tab name for this month
        tab_name = get_month_tab_name(month_key)
        if not tab_name:
            logging.error(f"Could not determine tab name for month {month_key}. Skipping.")
            continue

        # Ensure sheet exists
        ensure_sheet_exists(sheets_service, SHEET_ID, tab_name)

        # Clear existing data
        clear_sheet(sheets_service, SHEET_ID, tab_name)

        # Write new data
        write_to_sheet(sheets_service, SHEET_ID, tab_name, month_df)

    logging.info("All data has been written to Google Sheets.")

#############################################################################
# Entry Point
#############################################################################
if __name__ == "__main__":
    main()

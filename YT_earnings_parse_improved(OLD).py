#!/usr/bin/env python3

"""
YT Earnings Parsing Script - Improved
=====================================

Changes incorporated:
 1. Added Python logging instead of raw print statements for progress and warnings.
 2. Added more robust error handling when reading CSV files.
 3. Replaced AppleScript prompt with a function that tries AppleScript on macOS,
    otherwise falls back to a standard input() prompt.
 4. Parsed the date string (e.g. "1-2025") into a standardized YYYY-MM string.
 5. Used a single main() entry point.
 6. Organized the script into smaller functions for clarity.

Note: If you prefer the original AppleScript-only approach for user input, you
      can remove the fallback function.

"""

import os
import re
import io
import sys
import logging
import subprocess
from datetime import datetime

import pandas as pd
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

#############################################################################
# Configuration
#############################################################################

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
# Step 6) Main function
#############################################################################
def main():
    # Prompt user for folder URL.
    folder_url = ask_for_folder_url()
    # Parse folder ID.
    folder_id = parse_folder_id(folder_url)
    logging.info(f"Using folder ID: {folder_id}")

    # Authenticate with Google.
    gauth = GoogleAuth()
    gauth.LocalWebserverAuth()  # Opens or uses stored credentials
    drive = GoogleDrive(gauth)

    # Prepare a DataFrame to store unique channel data.
    all_channel_data = pd.DataFrame(columns=['Channel ID', 'Channel Display Name'])

    # List all CSV files in target folder.
    file_list = drive.ListFile({
        'q': f"'{folder_id}' in parents and mimeType='text/csv'"
    }).GetList()

    # We'll store aggregated rows in a list.
    all_rows = []

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

        # Append each row to aggregated list.
        for _, row in grouped_df.iterrows():
            all_rows.append({
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

    # Create final DataFrame.
    final_df = pd.DataFrame(all_rows)
    if final_df.empty:
        logging.warning("No valid data was aggregated. Exiting without creating output file.")
        sys.exit(0)

    # Fill missing Channel Display Names.
    def fill_display_name(row):
        if pd.notna(row["Channel Display Name"]) and row["Channel Display Name"]:
            return row["Channel Display Name"]
        else:
            return final_lookup.get(str(row["Channel ID"]), str(row["Channel ID"]))

    final_df["Channel Display Name"] = final_df.apply(fill_display_name, axis=1)

    # Additional columns.
    final_df["State"] = "Actual"
    final_df["1 - Category"] = "Revenue"
    final_df["2 - Subcategory"] = "Ad Revenue"
    final_df["Actual Date"] = final_df["date_str"]
    final_df["3 - Specifier"] = final_df["specifier"]
    final_df["4 - Detail"] = final_df["revenue_col"]

    final_df = final_df[
        [
            "State",
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

    # Save final CSV locally and upload.
    now_str = datetime.now().strftime("%Y-%m-%d")
    output_filename = f"Consolidated Earnings Sheet {now_str}.csv"
    local_path = f"/tmp/{output_filename}"
    final_df.to_csv(local_path, index=False)

    upload_file = drive.CreateFile({
        'title': output_filename,
        'parents': [{'id': folder_id}],
    })
    upload_file.SetContentFile(local_path)
    upload_file.Upload()

    logging.info(f"Consolidated data saved and uploaded to Drive as '{output_filename}'")

#############################################################################
# Entry Point
#############################################################################
if __name__ == "__main__":
    main()

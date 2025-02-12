#############################################################################
# 0) INSTALL & IMPORT PYDRIVE AND OTHER LIBRARIES
#############################################################################
# pip install pydrive

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

import pandas as pd
import io
import re
from datetime import datetime

#############################################################################
# 1) Prompt for the Google Drive folder URL and extract the folder ID
#############################################################################
folder_url = input("Enter the Google Drive folder URL: ").strip()

# Look for the folder ID pattern in the URL (after "/folders/")
match = re.search(r'/folders/([a-zA-Z0-9_-]+)', folder_url)
if match:
    folder_id = match.group(1)
    print(f"Using folder ID: {folder_id}")
else:
    print("Could not parse a folder ID from the URL. Exiting.")
    exit(1)

#############################################################################
# 2) Authenticate with Google Drive using your client secret in the working directory
#############################################################################
gauth = GoogleAuth()
# If your client secret file is named something else, set it explicitly:
# gauth.DEFAULT_SETTINGS['client_config_file'] = 'my_client_secret.json'
gauth.LocalWebserverAuth()  # Opens your browser for authentication
drive = GoogleDrive(gauth)

#############################################################################
# 3) Revenue column mapping (using your original mapping)
#############################################################################
revenue_column_map = {
    "Subscription Revenue Video Report": "Partner Revenue",
    "Paid Features Report": "Earnings (USD)",
    "Non Music Video Summary Premium": "Partner Revenue",
    "Shorts Ads Revenue": "Net Partner Revenue (Post revshare)",
    "Ads Revenue Video Summary": "Partner Revenue",
    "Ads Adjustment Report": "Partner Revenue",
    "Shorts Subscription Revenue": "Partner Revenue"
}

#############################################################################
# 4) List of filename prefixes for which we always skip the first row
#############################################################################
skip_first_row_prefixes = [
    "Subscription Revenue Video Report",
    "Paid Features Report",
    "Shorts Subscription Revenue Video Summary",
    "Premium Non Music Asset Video Summary"
]

#############################################################################
# 5) Helper: parse the new filename format and normalize the specifier
#
# Expected file names:
#   "Paid Features Report 1-2025.csv"          -> specifier: "Paid Features Report"
#   "Subscription Revenue Video Report 1-2025.csv" -> specifier: "Subscription Revenue Video Report"
#   "Shorts Ads Revenue Video Summary 1-2025.csv"   -> specifier: "Shorts Ads Revenue"
#   "Ads Revenue Video Summary 1-2025.csv"          -> specifier: "Ads Revenue Video Summary"
#   "Ads Adjustment Video Summary Report 1-2025.csv"-> specifier: "Ads Adjustment Report"
#   "Shorts Subscription Revenue Video Summary 1-2025.csv" -> specifier: "Shorts Subscription Revenue"
#   "Premium Non Music Asset Video Summary 1-2025.csv" -> specifier: "Non Music Video Summary Premium"
#############################################################################
def parse_filename(file_name: str):
    # Remove the .csv extension if present.
    if file_name.lower().endswith('.csv'):
        file_name = file_name[:-4].strip()
    
    parts = file_name.split()
    # Assume the last part is the date portion (e.g., "1-2025")
    date_str = parts[-1]
    raw_specifier = " ".join(parts[:-1])
    
    # Normalize the specifier to match the keys in revenue_column_map.
    if raw_specifier.startswith("Shorts Ads Revenue"):
        specifier = "Shorts Ads Revenue"
    elif raw_specifier.startswith("Ads Adjustment"):
        specifier = "Ads Adjustment Report"
    elif raw_specifier.startswith("Shorts Subscription Revenue"):
        specifier = "Shorts Subscription Revenue"
    elif raw_specifier.startswith("Premium Non Music Asset"):
        specifier = "Non Music Video Summary Premium"
    else:
        # Leave others unchanged.
        specifier = raw_specifier
    
    return date_str, specifier

#############################################################################
# 6) Function to read CSV content with optional forced skipping of the first row
#############################################################################
def try_read_csv(file_content: str, force_skip=False, skip_first_row_if_needed=True) -> pd.DataFrame:
    if force_skip:
        df = pd.read_csv(io.StringIO(file_content), skiprows=1)
        df.columns = [c.strip() for c in df.columns]
        return df
    else:
        # First, try reading without skipping rows.
        df = pd.read_csv(io.StringIO(file_content))
        df.columns = [c.strip() for c in df.columns]
        if "Channel ID" in df.columns:
            return df
        if skip_first_row_if_needed:
            df2 = pd.read_csv(io.StringIO(file_content), skiprows=1)
            df2.columns = [c.strip() for c in df2.columns]
            if "Channel ID" in df2.columns:
                return df2
        return None

#############################################################################
# 7) Helper: Extract channel mapping data from a DataFrame
#############################################################################
def extract_channel_data(df):
    if 'Channel ID' in df.columns and 'Channel Display Name' in df.columns:
        return df[['Channel ID', 'Channel Display Name']]
    return pd.DataFrame(columns=['Channel ID', 'Channel Display Name'])

all_channel_data = pd.DataFrame(columns=['Channel ID', 'Channel Display Name'])

#############################################################################
# 8) List all CSV files in the Drive folder
#############################################################################
file_list = drive.ListFile({
    'q': f"'{folder_id}' in parents and mimeType='text/csv'"
}).GetList()

# This will hold our aggregated rows.
all_rows = []

for f in file_list:
    file_name = f['title']  # e.g., "Paid Features Report 1-2025.csv"
    
    # Skip the output file if it exists in the folder.
    if file_name.startswith("Consolidated Earnings Sheet"):
        continue

    # Determine if we need to force skipping the first row.
    force_skip = any(file_name.startswith(prefix) for prefix in skip_first_row_prefixes)
    
    # Parse the date and specifier from the filename.
    date_str, specifier = parse_filename(file_name)
    
    # Look up the revenue column using the normalized specifier.
    revenue_col = revenue_column_map.get(specifier, None)
    if not revenue_col:
        print(f"[WARNING] No revenue column mapping found for '{file_name}' (specifier='{specifier}'). Skipping.")
        continue

    # Download the CSV file content.
    downloaded = f.GetContentString()
    
    # Read the CSV.
    df = try_read_csv(downloaded, force_skip=force_skip, skip_first_row_if_needed=True)
    if df is None:
        print(f"[WARNING] Could not parse '{file_name}' even after trying skiprows options. Skipping.")
        continue

    # Clean up column names.
    df.columns = [c.strip() for c in df.columns]

    # Aggregate channel mapping data.
    channel_subset = extract_channel_data(df)
    all_channel_data = pd.concat([all_channel_data, channel_subset], ignore_index=True).drop_duplicates()

    if "Channel ID" not in df.columns:
        print(f"[WARNING] 'Channel ID' column not found in '{file_name}'. Skipping.")
        continue

    # Group the data by Channel ID (and Channel Display Name if available).
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

    # Append each row to our aggregated list.
    for _, row in grouped_df.iterrows():
        all_rows.append({
            "date_str": date_str,
            "specifier": specifier,
            "Channel ID": row["Channel ID"],
            "Channel Display Name": row["Channel Display Name"] if pd.notna(row["Channel Display Name"]) else None,
            "Value": row["Value"],
            "revenue_col": revenue_col
        })

#############################################################################
# 9) Build a final Channel ID -> Display Name lookup
#############################################################################
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
    ['Channel Display Name'] +
    [f'Channel ID {i+1}' for i in range(expanded_channel_mapping.shape[1] - 1)]
)

final_lookup = {}
for _, row in expanded_channel_mapping.iterrows():
    display_name = row["Channel Display Name"]
    for col in row.index:
        if col.startswith("Channel ID"):
            ch_id = row[col]
            if pd.notna(ch_id) and str(ch_id).strip():
                final_lookup[str(ch_id).strip()] = display_name

#############################################################################
# 10) Create the final consolidated DataFrame
#############################################################################
final_df = pd.DataFrame(all_rows)

# Fill in missing Channel Display Names from the lookup.
final_df["Channel Display Name"] = final_df.apply(
    lambda r: r["Channel Display Name"]
              if pd.notna(r["Channel Display Name"]) and r["Channel Display Name"]
              else final_lookup.get(str(r["Channel ID"]), str(r["Channel ID"])),
    axis=1
)

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

#############################################################################
# 11) Save the final CSV locally and upload it to the Drive folder
#############################################################################
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

print(f"Consolidated data saved and uploaded to Drive as '{output_filename}'")

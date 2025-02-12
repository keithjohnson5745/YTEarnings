#############################################################################
# 0) INSTALL & IMPORT PYDRIVE (if not in Colab, you'll need a credentials flow)
#############################################################################
# pip install pydrive

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

import pandas as pd
import io
from datetime import datetime

#############################################################################
# 1) Authenticate and connect to Google Drive
#############################################################################
# If you're in a local environment, you'll need client_secrets.json, etc.
# If you're in Google Colab, you can use GoogleAuth default flow or `from google.colab import drive`

gauth = GoogleAuth()
gauth.LocalWebserverAuth()  # or use gauth.CommandLineAuth(), etc.
drive = GoogleDrive(gauth)

#############################################################################
# 2) Set your target folder ID
#    This is from the link: 
#    https://drive.google.com/drive/u/0/folders/1nLtii0QuxBflmLhaMUR6lhjJnrF46kTh
#############################################################################
folder_id = "1nLtii0QuxBflmLhaMUR6lhjJnrF46kTh"

#############################################################################
# 3) Dictionary for "Revenue" column (by specifier).
#    We'll match the *specifier* from the filename to get the correct col name.
#
#    E.g. a file named "Paid Features Report 1-2025.csv" => specifier="Paid Features"
#    Then you look up revenue_column_map["Paid Features"] => "Earnings (USD)"
#
#    Adjust as needed. If "Report" is part of specifier, adapt logic below.
#############################################################################
revenue_column_map = {
    "Paid Features": "Earnings (USD)",
    "Shorts Ads Revenue": "Net Partner Revenue (Post revshare)",
    "Ads Revenue Video Summary": "Partner Revenue",
    "Ads Adjustment": "Partner Revenue",
    "Subscription Revenue Video": "Partner Revenue",
    "Non Music Video Summary Premium": "Partner Revenue",
    "Shorts Subscription Revenue": "Partner Revenue",
    # add more if needed ...
}

#############################################################################
# 4) Helper: parse the new filename format
#    e.g. "Paid Features Report 1-2025.csv"
#    => date_str = "1-2025"
#       specifier = "Paid Features" (or "Paid Features Report" if we keep 'Report')
#
#    Adjust your logic to remove the trailing "Report" if you like.
#############################################################################
def parse_filename(file_name: str):
    # Remove '.csv'
    if file_name.lower().endswith('.csv'):
        file_name = file_name[:-4].strip()
        
    # Split on spaces
    parts = file_name.split()
    
    # Last "word" is presumably the date portion, e.g. '1-2025'
    date_str = parts[-1] 
    # Everything else is the specifier
    specifier = " ".join(parts[:-1])
    
    # OPTIONAL: If the specifier ends with "Report", remove that
    if specifier.endswith("Report"):
        specifier = specifier[: -len("Report")].strip()
        
    return date_str, specifier

#############################################################################
# 5) Try reading CSV from Drive with skiprows=0 or skiprows=1
#############################################################################
def try_read_csv(file_content: str, skip_first_row_if_needed=True) -> pd.DataFrame:
    """
    Attempt to parse 'file_content' as CSV with skiprows=0, 
    and if the needed columns are missing, try skiprows=1.
    Returns the best DataFrame it can get; if fails, returns None.
    """
    # First attempt: skiprows=0
    df = pd.read_csv(io.StringIO(file_content))
    df.columns = [c.strip() for c in df.columns]
    
    # If "Channel ID" column is found, we're good
    if "Channel ID" in df.columns:
        return df
    
    # If skip_first_row_if_needed is True, try again with skiprows=1
    if skip_first_row_if_needed:
        df2 = pd.read_csv(io.StringIO(file_content), skiprows=1)
        df2.columns = [c.strip() for c in df2.columns]
        if "Channel ID" in df2.columns:
            return df2
    
    return None

#############################################################################
# 6) Gather all channel IDs -> display names (like your old approach).
#    We'll store them in a big DataFrame and create a final_lookup dict
#############################################################################
def extract_channel_data(df):
    # If has columns [Channel ID, Channel Display Name], return that subset
    if 'Channel ID' in df.columns and 'Channel Display Name' in df.columns:
        return df[['Channel ID', 'Channel Display Name']]
    return pd.DataFrame(columns=['Channel ID', 'Channel Display Name'])

all_channel_data = pd.DataFrame(columns=['Channel ID', 'Channel Display Name'])

#############################################################################
# 7) List all CSV files in the Drive folder
#############################################################################
file_list = drive.ListFile({'q': f"'{folder_id}' in parents and mimeType='text/csv'"}).GetList()

# We'll store each final row for the consolidated DataFrame
all_rows = []

for f in file_list:
    file_name = f['title']  # e.g. "Paid Features Report 1-2025.csv"
    
    # Parse out the date & specifier from the filename
    date_str, specifier = parse_filename(file_name)
    
    # Check if we have a known revenue column for this specifier
    revenue_col = revenue_column_map.get(specifier, None)
    if not revenue_col:
        # If not found, you could skip or set a default. Let's just skip with a warning:
        print(f"[WARNING] No revenue column mapping found for '{file_name}' (specifier='{specifier}'). Skipping.")
        continue
    
    # Download file content from Drive
    downloaded = f.GetContentString()  # entire CSV as a string
    
    # Read the CSV (try skiprows=0, if needed skiprows=1)
    df = try_read_csv(downloaded, skip_first_row_if_needed=True)
    if df is None:
        print(f"[WARNING] Could not parse '{file_name}' even after trying skiprows=0 & skiprows=1. Skipping.")
        continue

    # Clean up the columns
    df.columns = [c.strip() for c in df.columns]
    
    # Attempt to extract channel data for the global map
    channel_subset = extract_channel_data(df)
    all_channel_data = pd.concat([all_channel_data, channel_subset], ignore_index=True).drop_duplicates()

    # Check that "Channel ID" is present
    if "Channel ID" not in df.columns:
        print(f"[WARNING] 'Channel ID' column not found in '{file_name}' after reading. Skipping.")
        continue

    # Summarize by Channel ID (and Channel Display Name if available)
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
        # Just group by Channel ID
        grouped_df = (
            df.groupby("Channel ID", dropna=False, as_index=False)[revenue_col]
              .sum()
              .rename(columns={revenue_col: "Value"})
        )
        # We'll fill in "Channel Display Name" later from our final lookup
        grouped_df["Channel Display Name"] = None

    # For each row in grouped_df, add to all_rows
    for _, row in grouped_df.iterrows():
        all_rows.append({
            "date_str": date_str,
            "specifier": specifier,
            "Channel ID": row["Channel ID"],
            "Channel Display Name": row["Channel Display Name"] if "Channel Display Name" in row else None,
            "Value": row["Value"],
            "revenue_col": revenue_col
        })

#############################################################################
# 8) Now that we've aggregated all channel data, build the final_lookup
#############################################################################
# Group by Display Name => gather all Channel IDs in a list
channel_mapping = (
    all_channel_data.groupby('Channel Display Name')['Channel ID']
    .apply(list)
    .reset_index()
)

# Expand that list out
expanded_channel_mapping = (
    channel_mapping
    .set_index('Channel Display Name')['Channel ID']
    .apply(pd.Series)
    .reset_index()
)

expanded_channel_mapping.columns = (
    ['Channel Display Name'] 
    + [f'Channel ID {i+1}' for i in range(expanded_channel_mapping.shape[1] - 1)]
)

# Build dict: each Channel ID -> Display Name
final_lookup = {}
for _, row in expanded_channel_mapping.iterrows():
    display_name = row["Channel Display Name"]
    for col in row.index:
        if col.startswith("Channel ID"):
            ch_id = row[col]
            if pd.notna(ch_id) and str(ch_id).strip():
                final_lookup[str(ch_id).strip()] = display_name

#############################################################################
# 9) Convert all_rows to a final DataFrame and fill in missing display names
#############################################################################
final_df = pd.DataFrame(all_rows)

# If "Channel Display Name" is None or empty, attempt to map from final_lookup
final_df["Channel Display Name"] = final_df.apply(
    lambda r: r["Channel Display Name"]
              if pd.notna(r["Channel Display Name"]) and r["Channel Display Name"] 
              else final_lookup.get(str(r["Channel ID"]), str(r["Channel ID"])), 
    axis=1
)

# Create the columns you want
final_df["State"] = "Actual"
final_df["1 - Category"] = "Revenue"
final_df["2 - Subcategory"] = "Ad Revenue"
final_df["Actual Date"] = final_df["date_str"]
final_df["3 - Specifier"] = final_df["specifier"]
final_df["4 - Detail"] = final_df["revenue_col"]

# Reorder columns
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
# 10) Save final DataFrame to CSV and upload to the same Drive folder
#############################################################################
now_str = datetime.now().strftime("%Y-%m-%d")  # or include time if you prefer
output_filename = f"Consolidated Earnings Sheet {now_str}.csv"

# Save locally first
local_path = f"/tmp/{output_filename}"  # or any local path
final_df.to_csv(local_path, index=False)

# Then upload to Drive
upload_file = drive.CreateFile({
    'title': output_filename,
    'parents': [{'id': folder_id}],  # put it in the same folder
})
upload_file.SetContentFile(local_path)
upload_file.Upload()

print(f"Consolidated data saved and uploaded to Drive as '{output_filename}'")

import pandas as pd
import os

# -------------------------
# 1) Define file paths
# -------------------------
file_paths = {
    "Shorts Ads Revenue": "/Users/keithjohnson/Downloads/CSVs to Run/1-2025 Shorts Ads Revenue Video Summary.csv",
    "Ads Revenue Video Summary": "/Users/keithjohnson/Downloads/CSVs to Run/1-2025 Ads Revenue Video Summary.csv",
    "Ads Adjustment Report": "/Users/keithjohnson/Downloads/CSVs to Run/1-2025 Ads Adjustment Video Summary Report.csv",
    "Subscription Revenue Video Report": "/Users/keithjohnson/Downloads/CSVs to Run/1-2025 Subscription Revenue Video Report.csv",
    "Paid Features Report": "/Users/keithjohnson/Downloads/CSVs to Run/1-2025 Paid Features Report.csv",
    "Non Music Video Summary Premium": "/Users/keithjohnson/Downloads/CSVs to Run/1-2025 Premium Non Music Video Asset Report.csv",
    "Shorts Subscription Revenue": "/Users/keithjohnson/Downloads/CSVs to Run/1-2025 Shorts Subscription Revenue Video Summary.csv"
}

# -------------------------
# 2) Files that need skiprows=1
# -------------------------
skiprows_files = [
    "Shorts Subscription Revenue",
    "Paid Features Report",
    "Subscription Revenue Video Report",
    "Non Music Video Summary Premium"
]

# -------------------------
# 3) Revenue column mapping
# -------------------------
revenue_column_map = {
    "Subscription Revenue Video Report": "Partner Revenue",
    "Paid Features Report": "Earnings (USD)",
    "Non Music Video Summary Premium": "Partner Revenue",
    "Shorts Ads Revenue": "Net Partner Revenue (Post revshare)",
    "Ads Revenue Video Summary": "Partner Revenue",
    "Ads Adjustment Report": "Partner Revenue",
    "Shorts Subscription Revenue": "Partner Revenue"
}

# ------------------------------------------------------------------
# 4) STEP A: Build the mapping of Channel ID -> Channel Display Name
# ------------------------------------------------------------------

def extract_channel_data(file_path, skiprows=False):
    """
    Reads a CSV from the given file_path, possibly skipping the first row,
    and returns a DataFrame with columns ['Channel ID', 'Channel Display Name']
    if they exist, otherwise an empty DataFrame.
    """
    try:
        if skiprows:
            df = pd.read_csv(file_path, skiprows=1)
        else:
            df = pd.read_csv(file_path)
        # Clean up column names just in case
        df.columns = [c.strip() for c in df.columns]

        if 'Channel ID' in df.columns and 'Channel Display Name' in df.columns:
            return df[['Channel ID', 'Channel Display Name']]
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
    
    return pd.DataFrame(columns=['Channel ID', 'Channel Display Name'])

# Collect all channel data from all files
all_channel_data = pd.DataFrame(columns=['Channel ID', 'Channel Display Name'])

for file_desc, file_path in file_paths.items():
    if os.path.exists(file_path):
        skiprows_flag = file_desc in skiprows_files
        channel_data = extract_channel_data(file_path, skiprows=skiprows_flag)
        # Combine, dropping duplicates
        all_channel_data = pd.concat([all_channel_data, channel_data], ignore_index=True).drop_duplicates()
    else:
        print(f"File not found: {file_path}")

# Group by Display Name => gather all Channel IDs in a list
channel_mapping = (
    all_channel_data.groupby('Channel Display Name')['Channel ID']
    .apply(list)
    .reset_index()
)

# Expand that list out so each Channel ID is in its own column
expanded_channel_mapping = (
    channel_mapping
    .set_index('Channel Display Name')['Channel ID']
    .apply(pd.Series)
    .reset_index()
)

# Rename columns: Channel Display Name, Channel ID 1, Channel ID 2, etc.
expanded_channel_mapping.columns = (
    ['Channel Display Name'] 
    + [f'Channel ID {i+1}' for i in range(expanded_channel_mapping.shape[1] - 1)]
)

# -------------------------------------------------------------------------
# 5) Optionally SAVE the mapping to a CSV (comment out if you don't need it)
# -------------------------------------------------------------------------
channel_map_csv = "Channel_ID_to_Display_Name_Mapping.csv"
expanded_channel_mapping.to_csv(channel_map_csv, index=False)
print(f"Channel mapping saved to {channel_map_csv}")


# --------------------------------------------------------------------------------
# 6) Build a dictionary that maps Channel IDs (from all columns) to Display Names
# --------------------------------------------------------------------------------
final_lookup = {}

for _, row in expanded_channel_mapping.iterrows():
    display_name = row["Channel Display Name"]
    
    # Go through each possible "Channel ID x" column
    for col in row.index:
        if col.startswith("Channel ID"):
            ch_id = row[col]
            if pd.notna(ch_id) and str(ch_id).strip():
                final_lookup[str(ch_id).strip()] = display_name


# -----------------------------------------------------------
# 7) Helper to parse the file name key for date & specifier
# -----------------------------------------------------------
def parse_filename_key(key: str):
    """
    Given something like "Dec 2024 Paid Features Report",
    returns ("Dec 2024", "Paid Features").
    """
    parts = key.split(" ", 2)
    date_str = " ".join(parts[:2])  # "Dec 2024"
    remainder = parts[2] if len(parts) > 2 else ""
    if remainder.endswith("Report"):
        remainder = remainder.replace("Report", "").strip()
    specifier = remainder.strip()
    return date_str, specifier

# -------------------------------------------
# 8) STEP B: Read and consolidate the data
# -------------------------------------------
all_rows = []

for key, path in file_paths.items():
    # Determine which column to sum
    revenue_col = revenue_column_map.get(key, None)
    if not revenue_col:
        print(f"[WARNING] No revenue column mapping found for '{key}'. Skipping.")
        continue

    # Decide whether we need to skip the first row
    should_skip = key in skiprows_files

    # Read CSV
    if os.path.exists(path):
        if should_skip:
            df = pd.read_csv(path, skiprows=1)
        else:
            df = pd.read_csv(path)
    else:
        print(f"[WARNING] File not found: {path}. Skipping.")
        continue

    # Clean up column names
    df.columns = [c.strip() for c in df.columns]

    # Ensure "Channel ID" column is present
    if "Channel ID" not in df.columns:
        print(f"[WARNING] 'Channel ID' column not found in '{key}'. Skipping.")
        continue

    # Parse the date & specifier from the key
    actual_date, specifier = parse_filename_key(key)

    # If the CSV has a "Channel" column, we'll group by both "Channel ID" & "Channel"
    if "Channel" in df.columns:
        grouped_df = (
            df.groupby(["Channel ID", "Channel"], dropna=False, as_index=False)[revenue_col]
              .sum()
        )
        # Rename them for uniformity
        grouped_df.rename(
            columns={
                "Channel": "Channel Display Name",
                revenue_col: "Value"
            }, 
            inplace=True
        )
        
        # If desired, you could override the display name with final_lookup:
        # grouped_df["Channel Display Name"] = grouped_df["Channel ID"].map(
        #     lambda cid: final_lookup.get(str(cid), row["Channel Display Name"])
        # )
        # That would forcibly override with the "official" name from the mapping.
        # For now, let's keep the name from the CSV itself.
    
    else:
        # No "Channel" column => group by Channel ID only
        grouped_df = (
            df.groupby("Channel ID", dropna=False, as_index=False)[revenue_col]
              .sum()
              .rename(columns={revenue_col: "Value"})
        )
        # Use final_lookup to fill in a "Channel Display Name"
        grouped_df["Channel Display Name"] = grouped_df["Channel ID"].apply(
            lambda cid: final_lookup.get(str(cid), str(cid))  # fallback to ID if not found
        )

    # Add each grouped record to our final list
    for _, row in grouped_df.iterrows():
        final_row = {
            "State": "Actual",
            "Channel ID": row["Channel ID"],
            "Channel Display Name": row["Channel Display Name"],
            "1 - Category": "Revenue",
            "2 - Subcategory": "Ad Revenue",
            "Actual Date": actual_date,
            "3 - Specifier": specifier,
            "4 - Detail": revenue_col,
            "Value": row["Value"]
        }
        all_rows.append(final_row)

# ----------------------------------------------------------------
# 9) Build the final DataFrame and reorder columns
# ----------------------------------------------------------------
final_df = pd.DataFrame(all_rows)

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
]

# ----------------------------------------------------------
# 10) Print and save the final consolidated CSV
# ----------------------------------------------------------
print("\nFinal Consolidated Data:")
print(final_df)

output_path = "/Users/keithjohnson/Downloads/Consolidated_Report4.csv"
final_df.to_csv(output_path, index=False)
print(f"Consolidated data saved to {output_path}")

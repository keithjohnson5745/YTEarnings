# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a YouTube earnings parsing and consolidation tool that:
- Authenticates with Google Drive using PyDrive
- Reads multiple CSV earnings reports from a specified Google Drive folder
- Consolidates earnings data from different report types (ads revenue, subscription revenue, paid features, etc.)
- Generates a unified earnings report with standardized formatting
- Uploads the consolidated report back to Google Drive

## Key Commands

### Installation
```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
source venv/bin/activate  # On macOS/Linux

# Install dependencies
pip install -r requirements.txt
```

### Running the Script
```bash
# Run the improved version (writes to CSV)
python YT_earnings_parse_improved.py

# Run the Google Sheets version (writes to Google Sheets)
python YT_earnings_parse_sheets.py

# Run the Google Sheets version with test folder
python YT_earnings_parse_sheets.py --test

# Run the original version (in archive)
python archive/YT_earnings_parse.py
```

### Building macOS App
```bash
# Build standalone app using py2app
python setup.py py2app
```

## Architecture Overview

### Main Components

1. **Authentication**: Uses PyDrive for Google Drive authentication via `client_secrets.json`
2. **CSV Processing**: Handles various YouTube earnings report formats with different column structures
3. **Data Consolidation**: Aggregates earnings by channel ID and report type
4. **Channel Mapping**: Maintains Channel ID to Display Name mappings across all reports

### Report Type Handling

The script recognizes these report types and their revenue columns:
- Subscription Revenue Video Report → "Partner Revenue"
- Paid Features Report → "Earnings (USD)"
- Non Music Video Summary Premium → "Partner Revenue"
- Shorts Ads Revenue → "Net Partner Revenue (Post revshare)"
- Ads Revenue Video Summary → "Partner Revenue"
- Ads Adjustment Report → "Partner Revenue"
- Shorts Subscription Revenue → "Partner Revenue"

Some report types require skipping the first row due to header formatting.

### Key Differences Between Versions

- **YT_earnings_parse_improved.py**: Enhanced version with logging, better error handling, cross-platform input prompts, and standardized date formatting (writes to CSV)
- **YT_earnings_parse_sheets.py**: Google Sheets version that writes data directly to tabs named in "mmm yy" format (e.g., "Jan 25")
- **archive/YT_earnings_parse.py**: Original version using AppleScript for user input (macOS only)

## Development Notes

- Google Drive authentication requires `client_secrets.json` in the project root
- The script expects CSV files with naming pattern: `[Report Type] [Month]-[Year].csv`
- Output file is named: `Consolidated Earnings Sheet [YYYY-MM-DD].csv`
- Temporary files are created in `/tmp/` before uploading to Drive
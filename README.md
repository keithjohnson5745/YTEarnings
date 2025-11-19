# YT Earnings Parsing Script - Google Sheets Version

This script is an automation tool designed to parse YouTube earnings reports (CSV files) from a Google Drive folder and consolidate them into a specific Google Sheet.

## Key Functions

### 1. Input & Authentication
*   **Prompts for Input:** It asks you for a Google Drive folder URL containing your earnings CSVs. On macOS, it tries to use a native popup dialog; otherwise, it uses the terminal input.
*   **Google Auth:** It handles authentication for both Google Drive (to read files) and Google Sheets (to write data).

### 2. Data Processing
*   **File Parsing:** It scans the folder for CSV files, parsing their filenames to determine the **Date** (Month/Year) and **Report Type** (e.g., "Ads Revenue", "Subscription Revenue").
*   **Data Extraction:** It reads each CSV, intelligently skipping header rows if necessary, and extracts:
    *   Channel ID & Name
    *   Revenue Amounts (mapping different report types to the correct revenue column)
*   **Aggregation:** It groups the data by Channel ID and Month, summing up the values.

### 3. Google Sheets Output
*   **Organized by Month:** It processes data month-by-month and targets specific tabs in your Google Sheet named in the format `"Mmm YY"` (e.g., "Jan 25").
*   **Smart Tab Management:**
    *   **Creates Tabs:** If a tab for a specific month doesn't exist, it creates it.
    *   **Clears Old Data:** It wipes the tab clean before writing new data to avoid duplicates.
*   **Formula Injection:** It doesn't just write static numbers; it writes **Excel/Google Sheets formulas** directly into the cells:
    *   `XLOOKUP` formulas to pull "Job Codes" and "Split Percentages" from a `JobCodeImport` tab.
*   **Expense Generation:** For every "Revenue" row it writes, it automatically generates a corresponding "Expense" row. This expense row calculates the payout amount using the split percentage formula.

## Summary of Output Columns

The script formats the final data with these specific columns:
*   **State:** (Set to "Actual")
*   **Job Code:** (Formula)
*   **Channel ID & Name**
*   **Category:** (Revenue/Expense)
*   **Subcategory:** (Ad Revenue/Payout)
*   **Actual Date**
*   **Specifier & Detail:** (Report type)
*   **Value:** (The dollar amount or formula)
*   **Current Split Lookup:** (Formula)

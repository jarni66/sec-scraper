import json
import requests
import pandas as pd
import time
import argparse
import os
import io
from requests.exceptions import Timeout, RequestException
# Import the DropboxManager from your dropbox_ops.py file
from dropbox_ops import DropboxManager

# --- CONFIGURATION ---
EMAILS = [
    "nizar.rizax@gmail.com",
    "project.mario.1@example.com"
]
REQUEST_TIMEOUT = 30

# DROPBOX CONFIG
# Replace these with your actual credentials or environment variables
APP_KEY = "dtm7p8v46wtwjh7"
APP_SECRET = "ocp7hvlybeyoyqg"
REFRESH_TOKEN = None 

# Initialize Dropbox Manager
dbx_handler = DropboxManager(APP_KEY, APP_SECRET, REFRESH_TOKEN)

def get_headers(email):
    return {
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9',
        'origin': 'https://www.sec.gov',
        'referer': 'https://www.sec.gov/',
        'user-agent': f'Mario bot project {email}',
    }

def upload_dataframe_to_dropbox(df, cik_padded):
    """Converts dataframe to CSV bytes and uploads to Dropbox."""
    if df.empty:
        return False
    
    dropbox_path = f"/Nizar/forms_table/{cik_padded}.csv"
    
    try:
        # Convert DF to CSV bytes in memory
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_bytes = csv_buffer.getvalue().encode('utf-8')
        
        # Upload using your DropboxManager
        meta = dbx_handler.upload_stream(csv_bytes, dropbox_path)
        return True if meta else False
    except Exception as e:
        print(f"  [Dropbox Error] Failed to upload {cik_padded}: {e}")
        return False

def process_cik_item(item, emails):
    """
    Processes a single row from the CSV. 
    item: a dictionary representing a row (must contain 'cik')
    """
    # Use .get() safely in case 'name' column doesn't exist in CSV
    original_name = item.get('name', 'Unknown')
    cik_raw = item.get('cik')
    
    if pd.isna(cik_raw):
        return {"error": "CIK is NaN"}, False

    # Ensure CIK is a 10-digit padded string
    cik_padded = str(int(cik_raw)).zfill(10)
    url = f'https://data.sec.gov/submissions/CIK{cik_padded}.json'
    
    last_error = "Unknown Error"

    for attempt in range(3):
        email = emails[attempt % len(emails)]
        headers = get_headers(email)
        
        try:
            # Rate limiting sleep (SEC allows 10 requests/sec)
            time.sleep(1.1) 
            
            response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            
            if response.status_code == 200:
                form_result = response.json()
                
                # 1. Extract 13F Dataframe
                df_13f = pd.DataFrame()
                if 'filings' in form_result and 'recent' in form_result['filings']:
                    df_all = pd.DataFrame(form_result['filings']['recent'])
                    if not df_all.empty and 'form' in df_all.columns:
                        # Filter for all 13F variations (13F-HR, 13F-NT, etc.)
                        df_13f = df_all[df_all['form'].str.contains('13F', na=False)].copy()
                
                # 2. Upload to Dropbox if 13F data exists
                if not df_13f.empty:
                    upload_success = upload_dataframe_to_dropbox(df_13f, cik_padded)
                    status_msg = "success + uploaded" if upload_success else "success + upload_failed"
                else:
                    status_msg = "success + no_13f_found"

                return {
                    "name": original_name,
                    "cik": cik_padded,
                    "13f_count": len(df_13f),
                    "status": status_msg
                }, True

            elif response.status_code == 429:
                print(f"[{cik_padded}] Rate limited by SEC. Sleeping 10s...")
                time.sleep(10)
            else:
                last_error = f"HTTP {response.status_code}"

        except (Timeout, RequestException) as e:
            last_error = str(e)
        
        print(f"[{cik_padded}] Attempt {attempt+1} failed: {last_error}")
        time.sleep(2)

    return {"name": original_name, "cik": cik_raw, "error": last_error}, False

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_csv", help="Path to the input CSV file containing a 'cik' column")
    args = parser.parse_args()

    if not os.path.exists(args.input_csv):
        print(f"File {args.input_csv} not found.")
        return

    # 1. Load the CSV
    try:
        df_input = pd.read_csv(args.input_csv)
        if 'cik' not in df_input.columns:
            print("Error: The CSV file must have a column named 'cik'.")
            return
        
        # Convert to list of dictionaries for processing
        data_to_process = df_input.to_dict('records')
    except Exception as e:
        print(f"Failed to read CSV: {e}")
        return

    # 2. Setup output naming based on input filename
    base_name = os.path.splitext(os.path.basename(args.input_csv))[0]
    output_success = f"{base_name}_results.json"
    output_failed = f"{base_name}_failed.json"

    success_list = []
    failed_list = []

    print(f"Starting process for {len(data_to_process)} CIKs from CSV...")
    
    try:
        for i, item in enumerate(data_to_process):
            # Process item (Fetch SEC -> Filter 13F -> Upload Dropbox)
            result, is_success = process_cik_item(item, EMAILS)
            
            if is_success:
                success_list.append(result)
            else:
                failed_list.append(result)

            # Print progress every 5 items
            if i % 5 == 0:
                print(f"Progress: {i}/{len(data_to_process)} | Success: {len(success_list)} | Failed: {len(failed_list)}")
            
            # Save progress to JSON periodically
            with open(output_success, 'w') as f:
                json.dump(success_list, f, indent=4)

    except KeyboardInterrupt:
        print("\n[!] Process interrupted by user. Progress saved.")

    # Final save for failures
    if failed_list:
        with open(output_failed, 'w') as f:
            json.dump(failed_list, f, indent=4)
        print(f"Saved {len(failed_list)} failures to {output_failed}")

    print(f"Finished. Total processed: {len(success_list)}. Check {output_success} for details.")

if __name__ == "__main__":
    main()

# Usage: python run_updated_table.py cik_to_run.csv
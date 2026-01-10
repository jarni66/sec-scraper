import json
import requests
import pandas as pd
import time
import argparse
import os
from requests.exceptions import Timeout, RequestException

# CONFIGURATION
EMAILS = [
    "nizar.rizax@gmail.com",
    "project.mario.1@example.com",
    "data.fetcher.pro@example.com",
    "research.bot@example.com"
]
REQUEST_TIMEOUT = 30 # Increased to 30 seconds

def get_headers(email):
    return {
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9',
        'origin': 'https://www.sec.gov',
        'referer': 'https://www.sec.gov/',
        'user-agent': f'Mario bot project {email}',
    }

def process_cik_item(item, emails):
    original_name = item.get('name')
    cik_raw = item.get('cik')
    cik_padded = str(cik_raw).zfill(10)
    url = f'https://data.sec.gov/submissions/CIK{cik_padded}.json'
    
    last_error = "Unknown Error"

    for attempt in range(3):
        email = emails[attempt % len(emails)]
        headers = get_headers(email)
        
        try:
            # IMPORTANT: Sleep is crucial when running multiple windows
            # 1.5s sleep per window ensures 11 windows don't exceed SEC rate limits
            time.sleep(1.5) 
            
            response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            
            if response.status_code == 200:
                form_result = response.json()
                filer_name = form_result.get("name")
                
                f13_count = 0
                if 'filings' in form_result and 'recent' in form_result['filings']:
                    df_forms = pd.DataFrame(form_result['filings']['recent'])
                    if not df_forms.empty and 'form' in df_forms.columns:
                        f13_count = int(df_forms['form'].str.contains('13F', na=False).sum())
                
                return {
                    "name": original_name,
                    "cik": cik_raw,
                    "filer_name": filer_name,
                    "13f_rows": f13_count,
                    "status": "success"
                }, True # Success flag

            else:
                last_error = f"HTTP {response.status_code}"
                print(f"[{cik_padded}] Attempt {attempt+1} failed: {last_error}")

        except Timeout:
            last_error = "Timeout"
            print(f"[{cik_padded}] Attempt {attempt+1}: Request timed out.")
        except RequestException as e:
            last_error = str(e)
            print(f"[{cik_padded}] Attempt {attempt+1}: Connection error.")
        
        time.sleep(2) # Wait a bit longer before retrying a failed request

    # If we reach here, all 3 attempts failed
    return {
        "name": original_name,
        "cik": cik_raw,
        "error": last_error
    }, False # Failure flag

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_file")
    args = parser.parse_args()

    if not os.path.exists(args.input_file):
        print(f"File {args.input_file} not found.")
        return

    # Generate output filenames
    # cik_chunks/cik_data_part_1.json -> cik_update_part_1.json
    base_name = os.path.basename(args.input_file)
    part_number = base_name.replace("cik_data_part_", "").replace(".json", "")
    
    output_success = f"cik_chunks/cik_update_part_{part_number}.json"
    output_failed = f"cik_chunks/failed_part_{part_number}.json"

    with open(args.input_file, 'r') as f:
        data = json.load(f)

    success_list = []
    failed_list = []

    print(f"Processing {len(data)} records from {base_name}...")
    
    try:
        for i, item in enumerate(data):
            result, is_success = process_cik_item(item, EMAILS)
            
            if is_success:
                success_list.append(result)
            else:
                failed_list.append(result)

            if i % 10 == 0:
                print(f"Progress: {i}/{len(data)} | Success: {len(success_list)} | Failed: {len(failed_list)}")
            
            
            # Save Successes
            with open(output_success, 'w') as f:
                json.dump(success_list, f, indent=4)
                
                
            # Save Failures (if any)
            if failed_list:
                with open(output_failed, 'w') as f:
                    json.dump(failed_list, f, indent=4)
                print(f"Saved {len(failed_list)} failures to {output_failed}")

    except KeyboardInterrupt:
        print("\nProcess interrupted by user. Saving progress...")


    print(f"Finished. Successfully processed {len(success_list)} records.")

if __name__ == "__main__":
    main()
    
# python process_cik_chunk.py cik_chunks/cik_data_part_1.json
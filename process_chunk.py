import csv
import sys
import os
import time
import random
import datetime
from pprint import pprint
from drive_ops import GoogleDriveManager
from dropbox_ops import DropboxManager

# --- CONFIGURATION ---
APP_KEY = "dtm7p8v46wtwjh7"
APP_SECRET = "ocp7hvlybeyoyqg"
DROPBOX_ROOT_PATH = "/Nizar/sec_forms"

# --- GLOBAL CLIENTS ---
DRIVE_CLIENT = None
DBX_CLIENT = None

def transfer_file_with_retry(file_id, target_path, max_retries=3):
    """Sequential transfer logic using global clients."""
    for attempt in range(1, max_retries + 1):
        try:
            # 1. Get Stream from Google
            file_stream = DRIVE_CLIENT.get_file_stream(file_id)
            
            if not file_stream:
                print(f"    [Attempt {attempt}] Google Drive returned empty stream.")
                time.sleep(2)
                continue

            # 2. Upload Stream to Dropbox
            result = DBX_CLIENT.upload_stream(file_stream, target_path)
            
            if hasattr(file_stream, 'close'):
                file_stream.close()

            if result:
                return True
            else:
                print(f"    [Attempt {attempt}] Dropbox upload returned None.")

        except Exception as e:
            print(f"    [Attempt {attempt}] Error: {str(e)}")
            sleep_time = (attempt * 2) + random.uniform(0.1, 1.0)
            time.sleep(sleep_time)
            
    return False

def main():
    global DRIVE_CLIENT, DBX_CLIENT

    # 1. Verify Argument
    if len(sys.argv) < 2:
        print("Usage: python process_chunk_sequential.py <path_to_chunk_csv>")
        return

    chunk_file = sys.argv[1]

    # 2. Initialize Managers
    try:
        DRIVE_CLIENT = GoogleDriveManager()
        DBX_CLIENT = DropboxManager(APP_KEY, APP_SECRET)
    except Exception as e:
        print(f"Initialization failed: {e}")
        return

    # 3. Read the Chunk CSV
    try:
        with open(chunk_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            all_rows = list(reader)
            total_items = len(all_rows)
    except Exception as e:
        print(f"Error reading chunk file: {e}")
        return

    print(f"\n--- Starting Sequential Migration for {chunk_file} ({total_items} items) ---")
    
    success_count = 0
    fail_count = 0
    start_time = time.time()
    existing_folders_cache = set()

    # 4. Process Rows One by One
    for i, row in enumerate(all_rows):
        current_num = i + 1
        
        folder_name = row['folder_name']
        file_name = row['file_name']
        file_id = row['fid']
        
        target_folder_path = f"{DROPBOX_ROOT_PATH}/{folder_name}"
        target_file_path = f"{target_folder_path}/{file_name}"
        
        print(f"[{current_num}/{total_items}] Processing: {folder_name}/{file_name}")

        try:
            # --- Ensure folder exists ---
            if folder_name not in existing_folders_cache:
                # We check this every time because we don't have a pre-built 
                # global cache like the main script, but we track it locally for this chunk.
                DBX_CLIENT.create_folder(target_folder_path)
                existing_folders_cache.add(folder_name)
            
            # --- Transfer ---
            if transfer_file_with_retry(file_id, target_file_path):
                print(f"  -> SUCCESS")
                success_count += 1
            else:
                print(f"  -> FAILED after retries")
                fail_count += 1
                pprint({
                    "file": file_name,
                    "folder": folder_name,
                    "fid": file_id,
                    "reason": "Max retries reached"
                })

        except Exception as e:
            print(f"  -> UNEXPECTED ERROR")
            fail_count += 1
            pprint({
                "file": file_name,
                "folder": folder_name,
                "error": str(e)
            })

    # 5. Summary
    duration = int(time.time() - start_time)
    print(f"\n--- Chunk Summary: {chunk_file} ---")
    print(f"Success: {success_count}")
    print(f"Failed:  {fail_count}")
    print(f"Total Time: {duration // 60}m {duration % 60}s")

if __name__ == "__main__":
    main()
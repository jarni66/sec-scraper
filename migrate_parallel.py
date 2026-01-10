import csv
import os
import time
import random
import threading
from pprint import pprint
from concurrent.futures import ThreadPoolExecutor, as_completed
from drive_ops import GoogleDriveManager
from dropbox_ops import DropboxManager

# --- CONFIGURATION ---
APP_KEY = "dtm7p8v46wtwjh7"
APP_SECRET = "ocp7hvlybeyoyqg"
INPUT_CSV = "drive_files.csv"
DROPBOX_ROOT_PATH = "/Nizar/sec_forms"

# THREADING CONFIG
BATCH_SIZE = 100  
MAX_WORKERS = 10  

# --- GLOBAL CLIENTS ---
DRIVE_CLIENT = None
DBX_CLIENT = None

# Stats
success_count = 0
fail_count = 0
processed_in_session = 0

def build_dropbox_lookup(dropbox_mgr, root_path):
    """Scans Dropbox ONCE to see what files and folders already exist."""
    print("--- 1. Pre-scanning Dropbox state... ---")
    metadata_list = dropbox_mgr.get_all_files_metadata(root_path, recursive=True)
    existing_files = set()
    existing_folders = set()
    
    for item in metadata_list:
        folder = item.get('folder_name')
        file = item.get('file_name')
        if folder and file:
            existing_files.add(f"{folder}/{file}")
            existing_folders.add(folder)
            
    print(f"Found {len(existing_folders)} folders and {len(existing_files)} files in Dropbox.")
    return existing_files, existing_folders

def transfer_file_worker(file_id, target_path, row_data, max_retries=3):
    """
    Uses global DRIVE_CLIENT and DBX_CLIENT to transfer files.
    Returns (success_boolean, row_data, error_message)
    """
    for attempt in range(1, max_retries + 1):
        try:
            file_stream = DRIVE_CLIENT.get_file_stream(file_id)
            if not file_stream:
                time.sleep(1)
                continue

            result = DBX_CLIENT.upload_stream(file_stream, target_path)
            
            if hasattr(file_stream, 'close'):
                file_stream.close()

            if result:
                print(f"    [Attempt {attempt}] Uploaded successfully.")
                return True, row_data, None
            
        except Exception as e:
            time.sleep((attempt * 2) + random.uniform(0.1, 1.0))
            if attempt == max_retries:
                return False, row_data, str(e)
            
    return False, row_data, "Max retries reached or empty stream"

def main():
    global DRIVE_CLIENT, DBX_CLIENT, success_count, fail_count, processed_in_session

    # 1. Initialize Global Managers
    try:
        DRIVE_CLIENT = GoogleDriveManager()
        DBX_CLIENT = DropboxManager(APP_KEY, APP_SECRET)
    except Exception as e:
        print(f"Initialization failed: {e}")
        return

    # 2. Build Cache
    existing_files_cache, existing_folders_cache = build_dropbox_lookup(DBX_CLIENT, DROPBOX_ROOT_PATH)
    
    # 3. Pre-filter and Folder Setup
    print(f"\n--- 2. Analyzing {INPUT_CSV} ---")
    all_pending_tasks = [] 
    
    try:
        with open(INPUT_CSV, 'r', encoding='utf-8') as f:
            rows = list(csv.DictReader(f))

        for row in rows:
            folder_name = row['folder_name']
            file_name = row['file_name']
            file_id = row['fid']
            
            unique_key = f"{folder_name}/{file_name}"
            target_folder_path = f"{DROPBOX_ROOT_PATH}/{folder_name}"
            target_file_path = f"{target_folder_path}/{file_name}"

            # Skip if already exists
            if unique_key in existing_files_cache:
                continue
            
            # Ensure folder exists upfront
            if folder_name not in existing_folders_cache:
                print(f"  -> Creating folder: {folder_name}")
                DBX_CLIENT.create_folder(target_folder_path)
                existing_folders_cache.add(folder_name)
            
            all_pending_tasks.append((file_id, target_file_path, row))

    except Exception as e:
        print(f"Error processing CSV data: {e}")
        return

    total_to_process = len(all_pending_tasks)
    if total_to_process == 0:
        print("Everything is already migrated.")
        return

    print(f"Items to transfer: {total_to_process}\n")
    start_time = time.time()

    # 4. Process in batches of 100
    for i in range(0, total_to_process, BATCH_SIZE):
        chunk = all_pending_tasks[i : i + BATCH_SIZE]
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_task = {
                executor.submit(transfer_file_worker, tid, tpath, trow): trow 
                for tid, tpath, trow in chunk
            }
            
            for future in as_completed(future_to_task):
                is_success, row_data, error_msg = future.result()
                processed_in_session += 1
                
                if is_success:
                    success_count += 1
                    print(f"[{processed_in_session}/{total_to_process}] SUCCESS: {row_data['file_name']}")
                else:
                    fail_count += 1
                    print(f"\n{'!'*20} TRANSFER FAILED {'!'*20}")
                    pprint({
                        "file": row_data['file_name'],
                        "folder": row_data['folder_name'],
                        "fid": row_data['fid'],
                        "error": error_msg
                    })
                    print(f"{'!'*56}\n")

    # Final Summary
    duration = int(time.time() - start_time)
    print("\n--- Migration Summary ---")
    print(f"Total Processed: {processed_in_session}")
    print(f"Success:         {success_count}")
    print(f"Failed:          {fail_count}")
    print(f"Time Taken:      {duration // 60}m {duration % 60}s")

if __name__ == "__main__":
    main()
import csv
import os
import time
import random
from drive_ops import GoogleDriveManager
from dropbox_ops import DropboxManager
import datetime
# --- CONFIGURATION ---
DROPBOX_TOKEN = "sl.u.AGLdlD4es_-S9KxBecFkl9zRYZ99dK_lUr0cHiDFvq3_vQ8um-o4MAr_oH6iPjnMJUSIN3b3qWYPZo2v5UHnsUsY2MIUdk-OehgZvgTtigk481YjnGwBlbeW5YCtDeMegyqdo-CctiDu9IiVp3iCFMZ0hFjI7pGBPQ4ZF2lEOyLeMb-VPgi2Q7zYEDOU-VnTKHaPJ27E3ra8mZORHhXN2VGs0_3QRyT4us4Prxe8tYsMaAkugN1-3zqEaExNRGFIG4p77IBatgRYw6BJazgffM2nB8iZMubuRHyEylJdAQEC2ZqLZYeycWnqEllGJEn3gW2io6juzcCZ5vltb1-yhOTJUbzCeSVOpO5DSJqT_M66S2xDG3yYh74SwATlp4lOm9kzo3_kjhi2PXtWwcJdWGEZwD0tWO_eoc2MZ_fm6Bk2-BSX8kwfBvWqss4MDwlwlhJx6yTv-npM9tu6Qj1L3kSYfiONxHMi-qqJNdiYC7EopVAL8fpnss5VYJvuWssMm3LZwPxbUsytKNbVhxVX_DVOyq6lTH3gWKo1EQmzHqSQc_JsyCp0-WriZCvobDYgBSsGB--_ocr1YP7tcY0Oq5zlfuQgxJrrVdFS2k7tTT_493g-yH-tqTcTPqcoub2qKVs0eGXoByEwC8wZlv5Gzrh4heZJFFsTNNsHkLRvsDqjrogiNZdDSnezO9YzgZDq2RIt0lzBy3rIqNsTaQKDa2PzTBxOkHw8BYZ7IuJbUPwpCI3v4f1ZwJi9udtZUE3zTA7W7hNDwsM0jyqyPvSMREcM_iOtddV0_aWHbmqeukc-ZhXoGHsmwbfbTE2FrMrbQJt23YasiWsDFgV1rJc3lje1ZHOAuaJBPqaeiyQ6IM5QRAaQEQKmcGZord5ZOigRftaRs7EHafz8qxQvLh-GT8gRdZWUhv_dmX2FZpav0boJgouXIPZ3akTYXJD6oU66rHCrhu7fcttE-WrtSMcONfG05adhWFqn7HZqMfaA-53J6fyEnyXlMbaGqYQc454BBfmTF1NvLiME9dLL9xZWc1ZiFcb02O-n2iEqY268IS2NkzcIOVS_I420c2sPp3gTmA8i9Ox8eYV1vILufPHSSIZZvKPR96T_FBZPgA54KQV0x2-hgSygiZIFpzc6kPbCH0RbqodZKNjaffJIJI3NOEV1Ab1KMOyZLfntkD9VPckv3utND3KthYUdCDuEY8KCsznsrDk8s4cBmgVO7KW9LNZtPx6ulrLqK5M37uRPSnYUfMWcWfuXAMd6PBQZFnNcfsbEJ4-4LuHLkZ-hOy7Ys4GHthIyqEZxP-xfCrVsSwOlHQaeUQ44hnV-s72j_YsTiJI"
INPUT_CSV = "drive_files.csv"
ERROR_LOG_CSV = "migration_failed.csv"
DROPBOX_ROOT_PATH = "/Nizar/sec_forms"

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

def transfer_file_with_retry(drive, dropbox, file_id, target_path, max_retries=3):
    """Attempts to stream download + upload with retries."""
    for attempt in range(1, max_retries + 1):
        try:
            # 1. Get Stream from Google
            # We fetch a FRESH stream every attempt
            file_stream = drive.get_file_stream(file_id)
            
            if not file_stream:
                print(f"    [Attempt {attempt}] Google Drive returned empty stream.")
                time.sleep(2)
                continue

            # 2. Upload Stream to Dropbox
            result = dropbox.upload_stream(file_stream, target_path)
            
            # Close stream to free memory
            file_stream.close()

            if result:
                return True
            else:
                print(f"    [Attempt {attempt}] Dropbox upload returned None.")

        except Exception as e:
            print(f"    [Attempt {attempt}] Error: {str(e)}")
            # Exponential Backoff: Wait 2s, then 4s, then 6s... + random jitter
            sleep_time = (attempt * 2) + random.uniform(0.1, 1.0)
            time.sleep(sleep_time)
            
    return False

def main():
    # 1. Initialize Managers
    try:
        drive = GoogleDriveManager()
        dbx = DropboxManager(DROPBOX_TOKEN)
    except Exception as e:
        print(f"Initialization failed: {e}")
        return

    # 2. Build Cache
    existing_files_cache, existing_folders_cache = build_dropbox_lookup(dbx, DROPBOX_ROOT_PATH)
    
    # 3. Process CSV
    print(f"\n--- 2. Starting Migration from {INPUT_CSV} ---")
    
    success_count = 0
    skip_count = 0
    fail_count = 0
    
    error_file_exists = os.path.exists(ERROR_LOG_CSV)
    
    try:
        with open(INPUT_CSV, 'r', encoding='utf-8') as f:
            # Step A: Read all rows into a list first so we can count them
            reader = csv.DictReader(f)
            all_rows = list(reader)
            total_items = len(all_rows)
            fieldnames = reader.fieldnames

        # Open error log separately
        with open(ERROR_LOG_CSV, 'a', newline='', encoding='utf-8') as err_f:
            err_writer = csv.DictWriter(err_f, fieldnames=fieldnames + ['error_reason'])
            if not error_file_exists:
                err_writer.writeheader()

            # Step B: Record Start Time
            start_time = time.time()

            for i, row in enumerate(all_rows):
                current_num = i + 1
                
                # --- ETA CALCULATION ---
                elapsed_time = time.time() - start_time
                # Avoid division by zero
                avg_time_per_item = elapsed_time / current_num if current_num > 0 else 0
                items_remaining = total_items - current_num
                eta_seconds = int(avg_time_per_item * items_remaining)
                eta_str = str(datetime.timedelta(seconds=eta_seconds))

                folder_name = row['folder_name']
                file_name = row['file_name']
                file_id = row['fid']
                
                unique_key = f"{folder_name}/{file_name}"
                target_folder_path = f"{DROPBOX_ROOT_PATH}/{folder_name}"
                target_file_path = f"{target_folder_path}/{file_name}"
                
                # Updated Print Statement
                print(f"Processing [{current_num}/{total_items}] (ETA: {eta_str}) : {unique_key}")

                try:
                    # --- CHECK 1: Skip if exists ---
                    if unique_key in existing_files_cache:
                        print(f"  -> Skipped (Exists)")
                        skip_count += 1
                        continue
                    
                    # --- CHECK 2: Create folder if needed ---
                    if folder_name not in existing_folders_cache:
                        print(f"  -> Creating folder: {folder_name}")
                        dbx.create_folder(target_folder_path)
                        existing_folders_cache.add(folder_name)
                    
                    # --- ACTION: Transfer with Retry ---
                    print(f"  -> Transferring...")
                    if transfer_file_with_retry(drive, dbx, file_id, target_file_path):
                        print(f"  -> SUCCESS")
                        existing_files_cache.add(unique_key)
                        success_count += 1
                    else:
                        print(f"  -> FAILED after retries")
                        row['error_reason'] = "Transfer failed after retries"
                        err_writer.writerow(row)
                        fail_count += 1

                except Exception as e:
                    print(f"  -> UNEXPECTED ERROR: {e}")
                    row['error_reason'] = str(e)
                    err_writer.writerow(row)
                    fail_count += 1
                    
    except FileNotFoundError:
        print(f"Error: Could not find file {INPUT_CSV}")
    except KeyboardInterrupt:
        print("\nScript stopped by user.")

    print("\n--- Migration Summary ---")
    print(f"Transferred: {success_count}")
    print(f"Skipped:     {skip_count}")
    print(f"Failed:      {fail_count}")
    print(f"Total Time:  {str(datetime.timedelta(seconds=int(time.time() - start_time)))}")
if __name__ == "__main__":
    main()
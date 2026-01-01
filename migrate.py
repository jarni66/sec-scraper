import csv
import os
import time
import random
from drive_ops import GoogleDriveManager
from dropbox_ops import DropboxManager
import datetime
# --- CONFIGURATION ---
APP_KEY = "dtm7p8v46wtwjh7"
APP_SECRET = "ocp7hvlybeyoyqg"
DROPBOX_TOKEN = "sl.u.AGIZbCsi7UBdGMUF4Z_OSq527MHzQg-ADax0IkMrPxYzuqb1AENghXF9me2cqsmVdI2V3_tbrbgDNG7_5rEd6kQvOgN33uzB7CSOfeszIorkY1Lry_ENruC7w7fQuvyUDvVD7MyugIAKIl8pDRqyFP7UZSjIyQlrrturTEdtXRJ-u0lUycidPpdmc3ufssofspHmAahREBi15150TfgYq8eBEf5rgnIeJdvsmIGVWDchDjDBEA0QmXGFSP5Cba6wh89baKVdKT3gUs9KpNw9RiHbumPzomfHW3RizMsbTZFgn8IK1A0BHrU5jb1l67bbT9m23Korjrl_x4mSGNV5BUFBuiK1l_MTTQUu2QwbmnseHMOO60rOvophkWOZ1X3288ZuA25evqC0IrtNqyA2BCtasc8wwaGGVrR2R7sah_fSW27jTpqeQeha7rP5oIqi60BGJVqvZekw-A-qdDGPSJ-PC_DPoyG0xuWQc9yEbpuQxiiMXQIGoWD3tmTN5aR0uc6D8pdN-XtN9reJh_9A05yuD88TAWjJ8Ie6ucF0dkgdALpcfYYAATP80XtFlzmnsDxv-3YdEdP70bUm3uX8CjX7LHvQfMJJZXiN0t1euHlvp4X1mCSrnU2Y_57Ie_QiC2Byz7GVeucz0pIj5932fQWdm1UAe09ET8FLjhhlg95yn1bTqwMZwxaGDyj99kdLltZjMQfu2MCtGDR1OuoYB2eEgS2MM9TF8rcFkz3dCLtz7h82uop1mPY8pg5LHKH5FoNuevfvHlP5sof6Dl9bqLv32gmgXwC66Gt7oxejXijV6fci0fhHU_1TzYe8NyUI_S70ygqXLGTERAkTAKxRn5biLza163y4CK-8_rS1U5hpX8FLXWmzqjrVnDvWF6ag8dWiw1l7Jdd-2m0GCfLtkQQ0O0LfHnfk_-1b22iedw4Ij2j9FZcbBL0ZWDvjUk_-CMjLACapvKuYjEXBRPp2O42GqdvYD2nsB8JEG8yq2K-QnvfQoqsc5NQ-Kayp2XODroJ35mtNbseN3p5efzN6NiariJCAr4NDGs8emRSChf-R_KJmkg-KO3NfW80vpou3HxNZYU9BBwFEKcMtXDaIkhi3ZZdiYX9dOqlA6pkomRhCWRYEpRiy7qwhprzLK-sgfu-PdhYgw9soHpKUx7q-uiKjVH1kQ0uqexplTV7JQYhRtGFfDY6evMTUNco98Qo_xj41d0MpwlU2BumuwRV3jpWhKdC0g6gDwkPtm1vlVPFns8lPhBPNj1lg-svGaNdHym-hYtfXnkFFxVFeXzqNAs2Ey63YMfTyXdqWNlX0Z4dQo3gpRTOf7prZPJqF_gy2j7c"
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
        dbx = DropboxManager(APP_KEY,APP_SECRET)
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
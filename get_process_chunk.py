import csv
import os
from dropbox_ops import DropboxManager

# --- CONFIGURATION ---
APP_KEY = "dtm7p8v46wtwjh7"
APP_SECRET = "ocp7hvlybeyoyqg"
INPUT_CSV = "drive_files.csv"
DROPBOX_ROOT_PATH = "/Nizar/sec_forms"
CHUNK_SIZE = 10000
OUTPUT_FOLDER = "pending_chunks"

def build_dropbox_lookup(dropbox_mgr, root_path):
    """Scans Dropbox to see what files already exist."""
    print("--- 1. Pre-scanning Dropbox state... ---")
    metadata_list = dropbox_mgr.get_all_files_metadata(root_path, recursive=True)
    existing_files = set()
    
    for item in metadata_list:
        folder = item.get('folder_name')
        file = item.get('file_name')
        if folder and file:
            existing_files.add(f"{folder}/{file}")
            
    print(f"Found {len(existing_files)} files already in Dropbox.")
    return existing_files

def main():
    # 1. Initialize Dropbox
    try:
        dbx = DropboxManager(APP_KEY, APP_SECRET)
    except Exception as e:
        print(f"Initialization failed: {e}")
        return

    # 2. Get existing files from Dropbox
    existing_files_cache = build_dropbox_lookup(dbx, DROPBOX_ROOT_PATH)

    # 3. Filter the main CSV
    print(f"--- 2. Filtering {INPUT_CSV} ---")
    files_to_upload = []
    
    if not os.path.exists(INPUT_CSV):
        print(f"Error: {INPUT_CSV} not found.")
        return

    with open(INPUT_CSV, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for row in reader:
            unique_key = f"{row['folder_name']}/{row['file_name']}"
            if unique_key not in existing_files_cache:
                files_to_upload.append(row)

    total_pending = len(files_to_upload)
    print(f"Total files needing upload: {total_pending}")

    if total_pending == 0:
        print("Nothing to process.")
        return

    # 4. Create Output Folder
    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)
        print(f"Created folder: {OUTPUT_FOLDER}")

    # 5. Chunking and Writing
    print(f"--- 3. Creating chunks of {CHUNK_SIZE} ---")
    chunk_count = 0
    for i in range(0, total_pending, CHUNK_SIZE):
        chunk_count += 1
        chunk = files_to_upload[i : i + CHUNK_SIZE]
        
        chunk_filename = os.path.join(OUTPUT_FOLDER, f"upload_chunk_{chunk_count}.csv")
        
        with open(chunk_filename, 'w', newline='', encoding='utf-8') as cf:
            writer = csv.DictWriter(cf, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(chunk)
            
        print(f"Generated: {chunk_filename} ({len(chunk)} rows)")

    print(f"\nDone! {chunk_count} CSV files are ready in the '{OUTPUT_FOLDER}' directory.")

if __name__ == "__main__":
    main()
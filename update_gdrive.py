from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from google.auth import default
import os
import io
import csv
import pandas as pd
import json

SCOPES = [
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/drive",
]

sec_folder_id = "1bjX-ozr5VDjsEsee8wr-mTow-u0iyr7_"
raw_txt_folder_id = "1JrP2wOw6-K5646kN8aP9iRo36OgCgG3v"

def build_service():
    creds, _ = default(scopes=SCOPES)
    return build("drive", "v3", credentials=creds)


def upload_file(folder_id: str, local_path: str, new_name: str | None = None):
    """Upload a file to Google Drive with its own service instance"""
    service = build_service()
    filename = new_name or os.path.basename(local_path)
    media = MediaFileUpload(local_path, resumable=True)
    metadata = {"name": filename, "parents": [folder_id]}
    return service.files().create(
        body=metadata,
        media_body=media,
        fields="id,name,size",
        supportsAllDrives=True
    ).execute()

def list_items_in_folder(service, folder_id):
    items = []
    page_token = None

    # includeItemsFromAllDrives/supportsAllDrives=True helps if the folder is
    # in a Shared Drive (it’s harmless for My Drive)
    while True:
        resp = service.files().list(
            q=f"'{folder_id}' in parents and trashed=false",
            fields="nextPageToken, files(id, name, mimeType, modifiedTime, size, owners(displayName,emailAddress))",
            pageSize=1000,
            pageToken=page_token,
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
        ).execute()
        items.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return items

def create_folder(service, parent_folder_id: str, name: str):

    metadata = {
        "name": name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_folder_id],
    }
    folder = service.files().create(
        body=metadata,
        fields="id,name",
        supportsAllDrives=True
    ).execute()
    return folder

def download_file(file_id: str, local_path: str):
    """Download a file from Google Drive to local path with its own service instance"""
    service = build_service()
    try:
        # Get file metadata
        file_metadata = service.files().get(fileId=file_id, supportsAllDrives=True).execute()
        file_name = file_metadata.get('name')
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        # Download the file
        request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        
        done = False
        while done is False:
            status, done = downloader.next_chunk()
        
        # Write to local file
        with open(local_path, 'wb') as f:
            f.write(fh.getvalue())
        
        return file_name
    except Exception as e:
        print(f"Error downloading file {file_id}: {str(e)}")
        return None



service = build_service()

# Verify it's your user and quota project is set
info = service.about().get(fields="user(emailAddress,displayName)").execute()
print(f"Authenticated as: {info['user']['displayName']} <{info['user']['emailAddress']}>")



def update_sec(file_data=None):
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading
    
    service = build_service()
    if not file_data:
        local_folder_date = os.listdir('sec_forms')
        drive_folder_list = list_items_in_folder(service, sec_folder_id)
        drive_folders = [i.get('name') for i in drive_folder_list]

        print("Checking Folders")
        for lf in local_folder_date:
            if lf not in drive_folders:
                create_folder(service, sec_folder_id, lf)
                print("Created folder :", lf)

        print("Checking Files")

        drive_folder_list = list_items_in_folder(service, sec_folder_id)
        
        # Collect all files that need to be uploaded
        files_to_upload = []
        for drf in drive_folder_list:
            folder_name = drf.get('name')
            folder_id = drf.get('id')

            item_list = list_items_in_folder(service, folder_id)
            item_list_names = [i.get('name') for i in item_list]

            # print(f"Drive item List {folder_name} :", item_list_names[:5])
            try:
                local_file_list = os.listdir(f"sec_forms/{folder_name}")
                for lcf in local_file_list:
                    if lcf not in item_list_names:
                        files_to_upload.append({
                            'folder_id': folder_id,
                            'local_path': f"sec_forms/{folder_name}/{lcf}",
                            'file_name': lcf
                        })
            except:
                pass
    else:
        files_to_upload = file_data
    
    # Upload files using thread pool
    if files_to_upload:
        # half = len(files_to_upload) // 2
        # first_half = files_to_upload[:half]
        # second_half = files_to_upload[half:]

        # with open("upload_part1.json", "w") as f:
        #     json.dump(first_half, f, indent=2)

        # with open("upload_part2.json", "w") as f:
        #     json.dump(second_half, f, indent=2)

        print(f"Uploading {len(files_to_upload)} files using thread pool...")
        
        def upload_single_file_threaded(file_info):
            try:
                result = upload_file(file_info['folder_id'], file_info['local_path'], file_info['file_name'])
                if result:
                    print("Upload :", file_info['local_path'])
                    return {"status": "success", "file": file_info['local_path']}
                else:
                    print("Failed :", file_info['local_path'])
                    return {"status": "failed", "file": file_info['local_path']}
            except Exception as e:
                print(f"Error uploading {file_info['local_path']}: {e}")
                return {"status": "error", "file": file_info['local_path'], "error": str(e)}
        
        # Use thread pool with limited workers to avoid rate limits
        with ThreadPoolExecutor(max_workers=25) as executor:
            futures = [executor.submit(upload_single_file_threaded, file_info) for file_info in files_to_upload]
            
            success_count = 0
            failed_count = 0
            for future in as_completed(futures):
                result = future.result()
                if result["status"] == "success":
                    success_count += 1
                else:
                    failed_count += 1
            
            print(f"Upload completed: {success_count} successful, {failed_count} failed")


    else:
        print("No files to upload")

def download_sec():
    """Download files from Google Drive sec_forms folder to local sec_forms directory"""
    drive_folder_list = list_items_in_folder(service, sec_folder_id)
    
    print("Checking Drive Folders")
    for drf in drive_folder_list:
        folder_name = drf.get('name')
        folder_id = drf.get('id')
        
        # Create local folder if it doesn't exist
        local_folder_path = f"sec_forms/{folder_name}"
        os.makedirs(local_folder_path, exist_ok=True)
        
        # Get files in the drive folder
        item_list = list_items_in_folder(service, folder_id)
        item_list_names = [i.get('name') for i in item_list]
        
        print(f"Drive item List {folder_name} :", item_list_names[:5])
        
        # Get local files in the folder
        try:
            local_file_list = os.listdir(local_folder_path)
        except FileNotFoundError:
            local_file_list = []
        
        print("Checking Files")
        for drive_file in item_list:
            file_name = drive_file.get('name')
            file_id = drive_file.get('id')
            local_file_path = f"{local_folder_path}/{file_name}"
            
            # Skip if it's a folder (Google Drive folders have mimeType 'application/vnd.google-apps.folder')
            if drive_file.get('mimeType') == 'application/vnd.google-apps.folder':
                continue
                
            # Download if file doesn't exist locally or if we want to update
            if file_name not in local_file_list:
                downloaded_name = download_file(file_id, local_file_path)
                if downloaded_name:
                    print("Downloaded :", f"{local_folder_path}/{downloaded_name}")
            else:
                print("File already exists locally :", f"{local_folder_path}/{file_name}")
    
def update_txt():
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading
    
    service = build_service()
    local_folder_date = os.listdir('raw_txt')
    drive_folder_list = list_items_in_folder(service, raw_txt_folder_id)
    drive_folders = [i.get('name') for i in drive_folder_list]

    print("Checking Folders")
    for lf in local_folder_date:
        if lf not in drive_folders:
            create_folder(service, raw_txt_folder_id, lf)
            print("Created folder :", lf)

    print("Checking Files")

    drive_folder_list = list_items_in_folder(service, raw_txt_folder_id)
    
    # Collect all files that need to be uploaded
    files_to_upload = []
    for drf in drive_folder_list:
        folder_name = drf.get('name')
        folder_id = drf.get('id')

        item_list = list_items_in_folder(service, folder_id)
        item_list_names = [i.get('name') for i in item_list]

        print(f"Drive item List {folder_name} :", item_list_names[:5])
        try:
            local_file_list = os.listdir(f"raw_txt/{folder_name}")
            for lcf in local_file_list:
                if lcf not in item_list_names:
                    files_to_upload.append({
                        'folder_id': folder_id,
                        'local_path': f"raw_txt/{folder_name}/{lcf}",
                        'file_name': lcf
                    })
        except:
            pass
    
    # Upload files using thread pool
    if files_to_upload:
        print(f"Uploading {len(files_to_upload)} files using thread pool...")
        
        def upload_single_file_threaded(file_info):
            try:
                result = upload_file(file_info['folder_id'], file_info['local_path'], file_info['file_name'])
                if result:
                    print("Upload :", file_info['local_path'])
                    return {"status": "success", "file": file_info['local_path']}
                else:
                    print("Failed :", file_info['local_path'])
                    return {"status": "failed", "file": file_info['local_path']}
            except Exception as e:
                print(f"Error uploading {file_info['local_path']}: {e}")
                return {"status": "error", "file": file_info['local_path'], "error": str(e)}
        
        # Use thread pool with limited workers to avoid rate limits
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(upload_single_file_threaded, file_info) for file_info in files_to_upload]
            
            success_count = 0
            failed_count = 0
            for future in as_completed(futures):
                result = future.result()
                if result["status"] == "success":
                    success_count += 1
                else:
                    failed_count += 1
            
            print(f"Upload completed: {success_count} successful, {failed_count} failed")
    else:
        print("No files to upload")
    
def download_txt():
    """Download files from Google Drive raw_txt folder to local raw_txt directory"""
    drive_folder_list = list_items_in_folder(service, raw_txt_folder_id)
    
    print("Checking Drive Folders")
    for drf in drive_folder_list:
        folder_name = drf.get('name')
        folder_id = drf.get('id')
        
        # Create local folder if it doesn't exist
        local_folder_path = f"raw_txt/{folder_name}"
        os.makedirs(local_folder_path, exist_ok=True)
        
        # Get files in the drive folder
        item_list = list_items_in_folder(service, folder_id)
        item_list_names = [i.get('name') for i in item_list]
        
        print(f"Drive item List {folder_name} :", item_list_names[:5])
        
        # Get local files in the folder
        try:
            local_file_list = os.listdir(local_folder_path)
        except FileNotFoundError:
            local_file_list = []
        
        print("Checking Files")
        for drive_file in item_list:
            file_name = drive_file.get('name')
            file_id = drive_file.get('id')
            local_file_path = f"{local_folder_path}/{file_name}"
            
            # Skip if it's a folder (Google Drive folders have mimeType 'application/vnd.google-apps.folder')
            if drive_file.get('mimeType') == 'application/vnd.google-apps.folder':
                continue
                
            # Download if file doesn't exist locally or if we want to update
            if file_name not in local_file_list:
                downloaded_name = download_file(file_id, local_file_path)
                if downloaded_name:
                    print("Downloaded :", f"{local_folder_path}/{downloaded_name}")
            else:
                print("File already exists locally :", f"{local_folder_path}/{file_name}")

def list_sec_files_to_csv(csv_filename="drive_sec_forms_file_list.csv"):
    """List all files in Google Drive sec_forms folder and save to CSV with folder_name and file_name columns"""
    drive_folder_list = list_items_in_folder(service, sec_folder_id)
    
    # Prepare data for CSV
    csv_data = []
    
    print("Scanning Google Drive sec_forms folder...")
    for drf in drive_folder_list:
        folder_name = drf.get('name')
        folder_id = drf.get('id')
        
        # Get files in the drive folder
        item_list = list_items_in_folder(service, folder_id)
        
        print(f"Processing folder: {folder_name} ({len(item_list)} items)")
        
        for drive_file in item_list:
            file_name = drive_file.get('name')
            file_id = drive_file.get('id')
            mime_type = drive_file.get('mimeType')
            
            # Skip if it's a folder (Google Drive folders have mimeType 'application/vnd.google-apps.folder')
            if mime_type == 'application/vnd.google-apps.folder':
                continue
            
            # Parse cik and acsn from file_name
            try:
                file_parts = file_name.split("_")
                if len(file_parts) >= 4:
                    cik = file_parts[0]
                    acsn = "-".join(file_parts[1:4])
                else:
                    cik = ""
                    acsn = ""
            except:
                cik = ""
                acsn = ""
            
            # Add to CSV data
            csv_data.append({
                'folder_name': folder_name,
                'file_name': file_name,
                'cik': cik,
                'acsn': acsn
            })
    
    # Write to CSV
    try:
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['folder_name', 'file_name', 'cik', 'acsn']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            # Write header
            writer.writeheader()
            
            # Write data
            writer.writerows(csv_data)
        
        print(f"Successfully saved {len(csv_data)} files to {csv_filename}")
        print(f"CSV contains files from {len(drive_folder_list)} folders")
        
    except Exception as e:
        print(f"Error writing CSV file: {str(e)}")
    
    return csv_data

def list_local_sec_files_to_csv(csv_filename="local_sec_forms_file_list.csv"):
    """List all files in local sec_forms directory and save to CSV with folder_name and file_name columns"""
    
    # Check if sec_forms directory exists
    if not os.path.exists('sec_forms'):
        print("Error: sec_forms directory not found!")
        return []
    
    # Prepare data for CSV
    csv_data = []
    
    print("Scanning local sec_forms directory...")
    
    try:
        # Get all folders in sec_forms directory
        local_folders = [f for f in os.listdir('sec_forms') if os.path.isdir(os.path.join('sec_forms', f))]
        
        for folder_name in local_folders:
            folder_path = os.path.join('sec_forms', folder_name)
            
            # Get all files in the folder
            try:
                files_in_folder = [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]
                
                print(f"Processing folder: {folder_name} ({len(files_in_folder)} files)")
                
                for file_name in files_in_folder:
                    # Parse cik and acsn from file_name
                    try:
                        file_parts = file_name.split("_")
                        if len(file_parts) >= 4:
                            cik = file_parts[0]
                            acsn = "-".join(file_parts[1:4])
                        else:
                            cik = ""
                            acsn = ""
                    except:
                        cik = ""
                        acsn = ""
                            
                    # Add to CSV data
                    csv_data.append({
                        'folder_name': folder_name,
                        'file_name': file_name,
                        'cik': cik,
                        'acsn': acsn
                    })
                                    
            except PermissionError:
                print(f"Permission denied accessing folder: {folder_name}")
                continue
            except Exception as e:
                print(f"Error accessing folder {folder_name}: {str(e)}")
                continue
    
    except Exception as e:
        print(f"Error scanning sec_forms directory: {str(e)}")
        return []
    
    # Write to CSV
    try:
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['folder_name', 'file_name', 'cik', 'acsn']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            # Write header
            writer.writeheader()
            
            # Write data
            writer.writerows(csv_data)
        
        print(f"Successfully saved {len(csv_data)} files to {csv_filename}")
        print(f"CSV contains files from {len(local_folders)} folders")
        
    except Exception as e:
        print(f"Error writing CSV file: {str(e)}")
    
    return csv_data

def merge_sec_files_csv(merged_filename="merged_sec_forms_file_list.csv", 
                       drive_csv="drive_sec_forms_file_list.csv", 
                       local_csv="local_sec_forms_file_list.csv"):
    """Run both Google Drive and local listing functions, merge the CSVs, remove duplicates, and save"""
    
    print("=== Starting merge process ===")
    
    # Step 1: Get Google Drive file list
    print("1. Getting Google Drive file list...")
    drive_data = list_sec_files_to_csv(drive_csv)
    
    # Step 2: Get local file list
    print("\n2. Getting local file list...")
    local_data = list_local_sec_files_to_csv(local_csv)
    
    # Step 3: Merge the data
    print("\n3. Merging file lists...")
    
    # Combine both datasets
    all_data = drive_data + local_data
    
    if not all_data:
        print("No data to merge!")
        return []
    
    # Convert to DataFrame for easier manipulation
    df = pd.DataFrame(all_data)
    
    # Add source column to track where each file came from
    df_drive = pd.DataFrame(drive_data)
    df_local = pd.DataFrame(local_data)
    
    if not df_drive.empty:
        df_drive['source'] = 'Google Drive'
    if not df_local.empty:
        df_local['source'] = 'Local'
    
    # Combine with source information
    df_combined = pd.concat([df_drive, df_local], ignore_index=True)
    
    # Remove duplicates based on folder_name and file_name
    print(f"Before deduplication: {len(df_combined)} files")
    df_merged = df_combined.drop_duplicates(subset=['folder_name', 'file_name'], keep='first')
    print(f"After deduplication: {len(df_merged)} files")
    print(f"Removed {len(df_combined) - len(df_merged)} duplicate files")
    
    # Step 4: Save merged CSV
    print(f"\n4. Saving merged file list to {merged_filename}...")
    
    try:
        # Save without source column for cleaner output
        df_output = df_merged[['folder_name', 'file_name', 'cik', 'acsn']].copy()
        df_output.to_csv(merged_filename, index=False)
        
        print(f"Successfully saved merged file list to {merged_filename}")
        print(f"Final CSV contains {len(df_output)} unique files")
        
        # Show summary statistics
        print("\n=== Summary ===")
        print(f"Google Drive files: {len(df_drive)}")
        print(f"Local files: {len(df_local)}")
        print(f"Total before deduplication: {len(df_combined)}")
        print(f"Unique files after merge: {len(df_output)}")
        
        # Show source breakdown
        if 'source' in df_merged.columns:
            source_counts = df_merged['source'].value_counts()
            print(f"\nSource breakdown:")
            for source, count in source_counts.items():
                print(f"  {source}: {count} files")
        
        return df_output.to_dict('records')
        
    except Exception as e:
        print(f"Error saving merged CSV: {str(e)}")
        return []

def upload_single_file(local_file_path):
    """
    Upload a single file to Google Drive sec_forms folder.
    Creates the folder if it doesn't exist.
    
    Args:
        local_file_path (str): Path to the local file (e.g., "sec_forms/1991_03_31/0001034882_0001047469_99_018673_1991_03_31.parquet")
    
    Returns:
        dict: Upload result with file ID and status
    """
    service = build_service()
    try:
        # Check if local file exists
        if not os.path.exists(local_file_path):
            print(f"Error: Local file not found: {local_file_path}")
            return {"status": "error", "message": "Local file not found"}
        
        # Extract folder name from path (e.g., "1991_03_31" from "sec_forms/1991_03_31/file.parquet")
        # Handle both forward and backward slashes
        path_parts = local_file_path.replace('\\', '/').split('/')
        if len(path_parts) < 3:
            print(f"Error: Invalid file path format: {local_file_path}")
            return {"status": "error", "message": "Invalid file path format"}
        
        folder_name = path_parts[-2]  # Second to last part is the folder name
        file_name = path_parts[-1]    # Last part is the file name
        
        print(f"Uploading file: {file_name} to folder: {folder_name}")
        
        # Check if folder exists in Google Drive
        drive_folder_list = list_items_in_folder(service, sec_folder_id)
        drive_folders = {folder.get('name'): folder.get('id') for folder in drive_folder_list}
        
        # Get or create folder
        if folder_name in drive_folders:
            folder_id = drive_folders[folder_name]
            print(f"Using existing folder: {folder_name}")
        else:
            print(f"Creating new folder: {folder_name}")
            folder = create_folder(service, sec_folder_id, folder_name)
            folder_id = folder.get('id')
            print(f"Created folder: {folder_name} with ID: {folder_id}")
        
        # Upload the file
        print(f"Uploading {file_name} to folder {folder_name}...")
        result = upload_file(folder_id, local_file_path, file_name)
        
        if result:
            print(f"Successfully uploaded: {file_name}")
            return {
                "status": "success",
                "file_id": result.get('id'),
                "file_name": result.get('name'),
                "file_size": result.get('size'),
                "folder_name": folder_name,
                "folder_id": folder_id
            }
        else:
            print(f"Failed to upload: {file_name}")
            return {"status": "error", "message": "Upload failed"}
            
    except Exception as e:
        error_msg = f"Error uploading file {local_file_path}: {str(e)}"
        print(error_msg)
        return {"status": "error", "message": error_msg}

def summarize_drive_parquet_files(csv_filename: str = "drive_sec_parquet_summary.csv"):
    """
    Iterate all subfolders under sec_folder_id, read each .parquet file from Drive
    in-memory, compute (row_count, column_count) and append to a single CSV.

    CSV columns: folder_name, file_name, rows, cols
    """
    service = build_service()
    drive_folder_list = list_items_in_folder(service, sec_folder_id)

    csv_exists = os.path.exists(csv_filename)
    
    # Read existing CSV to get set of already processed files
    processed_files = set()
    if csv_exists:
        try:
            with open(csv_filename, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    # Create a unique key from folder_name and file_name
                    folder_name = row.get('folder_name', '')
                    file_name = row.get('file_name', '')
                    if folder_name and file_name:
                        processed_files.add((folder_name, file_name))
            print(f"Found {len(processed_files)} already processed files in CSV")
        except Exception as e:
            print(f"Warning: Could not read existing CSV to check for duplicates: {str(e)}")
    
    try:
        with open(csv_filename, 'a', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['folder_name', 'file_name', 'rows', 'cols']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if not csv_exists:
                writer.writeheader()

            for drf in drive_folder_list:
                folder_name = drf.get('name')
                folder_id = drf.get('id')

                item_list = list_items_in_folder(service, folder_id)

                for drive_file in item_list:
                    mime_type = drive_file.get('mimeType')
                    if mime_type == 'application/vnd.google-apps.folder':
                        continue

                    file_name = drive_file.get('name')
                    print("file_name", file_name)
                    # Only parquet files
                    if not (file_name and file_name.lower().endswith('.parquet')):
                        continue

                    # Check if file already exists in CSV
                    if (folder_name, file_name) in processed_files:
                        print(f"Skipping {folder_name}/{file_name}: already exists in CSV")
                        continue

                    file_id = drive_file.get('id')

                    try:
                        # Download file content into memory
                        request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
                        fh = io.BytesIO()
                        downloader = MediaIoBaseDownload(fh, request)
                        done = False
                        while done is False:
                            _, done = downloader.next_chunk()

                        fh.seek(0)
                        # Read parquet and compute shape
                        df = pd.read_parquet(fh)
                        rows, cols = df.shape

                        writer.writerow({
                            'folder_name': folder_name,
                            'file_name': file_name,
                            'rows': rows,
                            'cols': cols,
                        })
                        # Add to processed_files set to avoid duplicates in same run
                        processed_files.add((folder_name, file_name))
                        print(f"Processed {folder_name}/{file_name}: rows={rows}, cols={cols}")
                    except Exception as e:
                        print(f"Failed processing {folder_name}/{file_name}: {str(e)}")
                        # Continue with next file
                        continue
    except Exception as e:
        print(f"Error writing to CSV {csv_filename}: {str(e)}")
        return []

    print(f"Completed summary. Results appended to {csv_filename}")
    return csv_filename



def check_files_count():
    service = build_service()
    drive_folder_list = list_items_in_folder(service, sec_folder_id)
    results = []
    for i, drf in enumerate(drive_folder_list):
        folder_name = drf.get('name')
        folder_id = drf.get('id')
        print("Folder :", folder_name)
        print("Process :", f"{i+1}/{len(drive_folder_list)}")
        
        item_list = list_items_in_folder(service, folder_id)
        results.append(
            {
                "folder_name" : folder_name,
                "folder_id" : folder_id,
                "items" : item_list
            }
        )
        with open("check_files_count.json", "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=4)




# Example usage:
# To download files from Google Drive to local:
# download_sec()  # Downloads sec_forms from Google Drive to local sec_forms folder
# download_txt()  # Downloads raw_txt from Google Drive to local raw_txt folder

# To upload files from local to Google Drive (existing functionality):
# update_sec()    # Uploads local sec_forms to Google Drive
# update_txt()    # Uploads local raw_txt to Google Drive

# To list all files in Google Drive sec_forms folder to CSV:
# list_sec_files_to_csv()  # Creates sec_forms_file_list.csv with folder_name and file_name columns
# list_sec_files_to_csv("custom_filename.csv")  # Creates CSV with custom filename

# To list all files in local sec_forms directory to CSV:
# list_local_sec_files_to_csv()  # Creates local_sec_forms_file_list.csv with folder_name and file_name columns
# list_local_sec_files_to_csv("custom_local_filename.csv")  # Creates CSV with custom filename

# To merge Google Drive and local file lists:
# merge_sec_files_csv()  # Creates merged_sec_forms_file_list.csv with deduplicated files
# merge_sec_files_csv("custom_merged_filename.csv")  # Creates merged CSV with custom filename

# To upload a single file to Google Drive:
# upload_single_file("sec_forms/1991_03_31/0001034882_0001047469_99_018673_1991_03_31.parquet")

# Uncomment the function you want to run:
# download_sec()
# download_txt()
# list_sec_files_to_csv()
# list_local_sec_files_to_csv()
# merge_sec_files_csv()
# upload_single_file("sec_forms/1991_03_31/0001034882_0001047469_99_018673_1991_03_31.parquet")
# with open("upload_part1.json", "r", encoding="utf-8") as f:
#     upload_part1 = json.load(f)

# while True:
#     try:
#         download_sec()
#         break
#     except:
#         pass

summarize_drive_parquet_files()
# check_files_count()
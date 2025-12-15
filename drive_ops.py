import os
import io
import csv
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseUpload, MediaIoBaseDownload
from google.auth import default
from googleapiclient.errors import HttpError

class GoogleDriveManager:
    """
    A wrapper class for Google Drive API v3 operations.
    Covers: CRUD, Smart Upload (Upsert), Listing with Metadata, and Stream Downloading.
    """
    
    SCOPES = ["https://www.googleapis.com/auth/drive"]

    def __init__(self):
        """Initialize authentication using Application Default Credentials."""
        try:
            creds, _ = default(scopes=self.SCOPES)
            self.service = build("drive", "v3", credentials=creds)
            
            # Verify connection
            about = self.service.about().get(fields="user(displayName,emailAddress)").execute()
            user = about.get('user', {})
            print(f"Connected to Google Drive as: {user.get('displayName')} <{user.get('emailAddress')}>")
            
        except Exception as e:
            print(f"Failed to authenticate with Google Drive: {e}")
            raise e

    # ==========================================
    # 1. LISTING & METADATA (READ)
    # ==========================================

    def get_all_files_metadata(self, folder_id):
        """
        Retrieves metadata for ALL files in a specific folder ID (handling pagination).
        Returns a list of dictionaries.
        """
        print(f"Scanning Drive folder ID: {folder_id}...")
        files_data = []
        page_token = None

        try:
            while True:
                # Query: inside folder, not trash
                query = f"'{folder_id}' in parents and trashed=false"
                
                resp = self.service.files().list(
                    q=query,
                    # Request specific metadata fields
                    fields="nextPageToken, files(id, name, mimeType, size, modifiedTime, createdTime, owners(displayName, emailAddress))",
                    pageSize=1000,
                    pageToken=page_token,
                    includeItemsFromAllDrives=True,
                    supportsAllDrives=True
                ).execute()

                items = resp.get("files", [])
                
                for item in items:
                    # Flatten the data for CSV friendliness
                    owner_name = item.get('owners', [{}])[0].get('displayName', 'Unknown')
                    
                    files_data.append({
                        "file_id": item.get("id"),
                        "file_name": item.get("name"),
                        "mime_type": item.get("mimeType"),
                        "size_bytes": item.get("size", "0"),
                        "modified_time": item.get("modifiedTime"),
                        "created_time": item.get("createdTime"),
                        "owner": owner_name
                    })

                page_token = resp.get("nextPageToken")
                if not page_token:
                    break
            
            print(f"Scan complete. Found {len(files_data)} items.")
            return files_data

        except HttpError as e:
            print(f"Error listing folder: {e}")
            return []

    def save_metadata_to_csv(self, folder_id, output_csv="gdrive_files.csv"):
        """Wraps get_all_files_metadata and saves to CSV."""
        data = self.get_all_files_metadata(folder_id)
        
        if not data:
            print("No data to save.")
            return

        keys = data[0].keys()
        
        try:
            with open(output_csv, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=keys)
                writer.writeheader()
                writer.writerows(data)
            print(f"Metadata saved to: {output_csv}")
        except IOError as e:
            print(f"Error saving CSV: {e}")

    # ==========================================
    # 2. CREATE / UPDATE (UPLOAD)
    # ==========================================

    def _find_file_id_by_name(self, name, parent_folder_id):
        """Helper: Checks if a file exists in a folder to enable 'Update' logic."""
        query = f"name = '{name}' and '{parent_folder_id}' in parents and trashed = false"
        resp = self.service.files().list(
            q=query, fields="files(id)", pageSize=1,
            includeItemsFromAllDrives=True, supportsAllDrives=True
        ).execute()
        files = resp.get('files', [])
        return files[0]['id'] if files else None

    def upload_file(self, local_path, parent_folder_id, force_update=True):
        """
        Smart Upload:
        - If file exists in folder -> Updates content (keeps ID).
        - If file does not exist -> Creates new file.
        """
        if not os.path.exists(local_path):
            print(f"Local file not found: {local_path}")
            return None

        filename = os.path.basename(local_path)
        
        # 1. Check if file already exists
        existing_file_id = self._find_file_id_by_name(filename, parent_folder_id)

        media = MediaFileUpload(local_path, resumable=True)

        try:
            if existing_file_id and force_update:
                print(f"File exists ({existing_file_id}). Updating...")
                # UPDATE existing file
                file = self.service.files().update(
                    fileId=existing_file_id,
                    media_body=media,
                    fields="id, name",
                    supportsAllDrives=True
                ).execute()
                print(f"Updated: {file.get('name')}")
            else:
                print(f"Uploading new file: {filename}...")
                # CREATE new file
                file_metadata = {'name': filename, 'parents': [parent_folder_id]}
                file = self.service.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields="id, name",
                    supportsAllDrives=True
                ).execute()
                print(f"Created: {file.get('name')} ID: {file.get('id')}")
            
            return file

        except HttpError as e:
            print(f"Upload failed: {e}")
            return None

    def create_folder(self, folder_name, parent_folder_id):
        """Creates a folder if it doesn't exist, otherwise returns existing ID."""
        existing_id = self._find_file_id_by_name(folder_name, parent_folder_id)
        
        if existing_id:
            print(f"Folder already exists: {folder_name} ({existing_id})")
            return {"id": existing_id, "name": folder_name}
        
        print(f"Creating folder: {folder_name}")
        metadata = {
            "name": folder_name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_folder_id]
        }
        try:
            folder = self.service.files().create(
                body=metadata, fields="id, name", supportsAllDrives=True
            ).execute()
            return folder
        except HttpError as e:
            print(f"Folder creation failed: {e}")
            return None

    # ==========================================
    # 3. READ (DOWNLOAD)
    # ==========================================

    def get_file_stream(self, file_id):
        """
        Downloads a file into an in-memory BytesIO object.
        Perfect for streaming directly to Dropbox without disk usage.
        """
        try:
            request = self.service.files().get_media(fileId=file_id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            
            done = False
            while not done:
                status, done = downloader.next_chunk()
                # print(f"Download progress: {int(status.progress() * 100)}%")
            
            fh.seek(0) # Reset pointer to start
            return fh
        except HttpError as e:
            print(f"Stream download failed: {e}")
            return None

    def download_file_to_disk(self, file_id, local_path):
        """Downloads a file to local disk."""
        stream = self.get_file_stream(file_id)
        if stream:
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            with open(local_path, "wb") as f:
                f.write(stream.read())
            print(f"Downloaded to: {local_path}")
            return True
        return False

    # ==========================================
    # 4. DELETE
    # ==========================================

    def delete_file(self, file_id):
        """Permanently deletes a file or folder."""
        try:
            self.service.files().delete(fileId=file_id, supportsAllDrives=True).execute()
            print(f"Deleted file ID: {file_id}")
            return True
        except HttpError as e:
            print(f"Delete failed: {e}")
            return False

# ==========================================
# EXAMPLE USAGE
# ==========================================

if __name__ == "__main__":
    # 1. Initialize
    try:
        # Ensure you ran: gcloud auth application-default login --scopes="https://www.googleapis.com/auth/drive"
        gdm = GoogleDriveManager()
        
        folder_id = "1bjX-ozr5VDjsEsee8wr-mTow-u0iyr7_" # Example Folder ID
        
        # --- A. List files & Save CSV ---
        # gdm.save_metadata_to_csv(folder_id, "drive_report.csv")
        
        # --- B. Create Folder ---
        # new_folder = gdm.create_folder("New_Test_Folder", folder_id)
        # new_folder_id = new_folder['id']
        
        # --- C. Upload Local File (Upsert) ---
        # gdm.upload_file("local_test.txt", new_folder_id)
        
        # --- D. Stream Download (For Dropbox Transfer) ---
        # file_id_to_move = "SOME_FILE_ID"
        # stream = gdm.get_file_stream(file_id_to_move)
        # if stream:
        #    print("Got stream, ready to send to Dropbox...")
        
        # --- E. Delete ---
        # gdm.delete_file("SOME_FILE_ID")
        
    except Exception as e:
        print(f"Script stopped: {e}")
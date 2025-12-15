import dropbox
import dropbox.files
from dropbox.exceptions import ApiError, AuthError
import os
import csv
import io

class DropboxManager:
    """
    A wrapper class for Dropbox API v2 operations.
    Covers: CRUD, Listing (Recursive & Non-Recursive), and Stream Uploading.
    """

    def __init__(self, access_token):
        """Initialize connection to Dropbox."""
        try:
            self.dbx = dropbox.Dropbox(access_token)
            account = self.dbx.users_get_current_account()
            print(f"Connected to Dropbox as: {account.name.display_name}")
        except AuthError as e:
            print("Error: Invalid Access Token")
            raise e

    # ==========================================
    # 1. LISTING & METADATA (READ)
    # ==========================================

    def get_all_files_metadata(self, folder_path, recursive=True):
        """
        Retrieves metadata for ALL files recursively.
        Extracts 'folder_name' (immediate parent) for CSV grouping.
        """
        print(f"Recursively scanning folder: {folder_path}...")
        files_data = []
        
        try:
            if folder_path == "/" or folder_path == ".": 
                folder_path = ""
                
            result = self.dbx.files_list_folder(folder_path, recursive=recursive)
            
            def process_entries(entries):
                for entry in entries:
                    if isinstance(entry, dropbox.files.FileMetadata):
                        # Extract folder name from path (e.g., /Root/DateFolder/File -> DateFolder)
                        # Dropbox always uses forward slashes
                        path_parts = entry.path_display.split('/')
                        
                        # The last part is filename, second to last is the folder name
                        folder_name = path_parts[-2] if len(path_parts) > 1 else ""

                        files_data.append({
                            "folder_name": folder_name,  # <--- Added this field
                            "file_name": entry.name,
                            "path_display": entry.path_display,
                            "size_bytes": entry.size,
                            "server_modified": entry.server_modified.isoformat(),
                            "id": entry.id
                        })

            process_entries(result.entries)

            # Pagination
            while result.has_more:
                print(f"Fetching more items... (Current count: {len(files_data)})")
                result = self.dbx.files_list_folder_continue(result.cursor)
                process_entries(result.entries)

            print(f"Scan complete. Found {len(files_data)} files.")
            return files_data

        except ApiError as e:
            print(f"Error listing folder {folder_path}: {e}")
            return []

    def save_metadata_to_csv(self, folder_path, output_csv="dropbox_files.csv"):
        """
        Wraps get_all_files_metadata and saves to CSV including folder_name.
        """
        data = self.get_all_files_metadata(folder_path)
        
        if not data:
            print("No data to save.")
            return

        # Define explicit order for CSV columns
        fieldnames = ["folder_name", "file_name", "size_bytes", "server_modified", "path_display", "id"]
        
        try:
            with open(output_csv, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(data)
            print(f"Metadata saved to: {output_csv}")
        except IOError as e:
            print(f"Error saving CSV: {e}")

    # ==========================================
    # 2. OTHER METHODS (Unchanged)
    # ==========================================

    def list_immediate_contents(self, folder_path):
        """Lists ONLY the immediate children (files and subfolders)."""
        print(f"Listing immediate contents of: {folder_path}...")
        items_data = []
        try:
            if folder_path == "/" or folder_path == ".": folder_path = ""
            result = self.dbx.files_list_folder(folder_path, recursive=False)
            
            def process_entries(entries):
                for entry in entries:
                    item = {
                        "name": entry.name,
                        "type": "unknown",
                        "path_display": entry.path_display,
                        "id": entry.id
                    }
                    if isinstance(entry, dropbox.files.FileMetadata):
                        item["type"] = "file"
                    elif isinstance(entry, dropbox.files.FolderMetadata):
                        item["type"] = "folder"
                    items_data.append(item)

            process_entries(result.entries)
            while result.has_more:
                result = self.dbx.files_list_folder_continue(result.cursor)
                process_entries(result.entries)
            return items_data
        except ApiError as e:
            print(f"Error listing: {e}")
            return []

    def create_folder(self, dropbox_path):
        try:
            meta = self.dbx.files_create_folder_v2(dropbox_path)
            print(f"Created folder: {meta.metadata.name}")
            return meta
        except ApiError as e:
            print(f"Folder creation issue: {e}")
            return None

    def upload_stream(self, file_bytes, dropbox_path):
        try:
            if hasattr(file_bytes, 'read'): data = file_bytes.read()
            else: data = file_bytes
            meta = self.dbx.files_upload(data, dropbox_path, mode=dropbox.files.WriteMode("overwrite"))
            return meta
        except ApiError as e:
            print(f"Stream upload failed: {e}")
            return None

# ==========================================
# EXAMPLE USAGE
# ==========================================
if __name__ == "__main__":
    # Replace with your token
    TOKEN = "sl.u.AGLdlD4es_-S9KxBecFkl9zRYZ99dK_lUr0cHiDFvq3_vQ8um-o4MAr_oH6iPjnMJUSIN3b3qWYPZo2v5UHnsUsY2MIUdk-OehgZvgTtigk481YjnGwBlbeW5YCtDeMegyqdo-CctiDu9IiVp3iCFMZ0hFjI7pGBPQ4ZF2lEOyLeMb-VPgi2Q7zYEDOU-VnTKHaPJ27E3ra8mZORHhXN2VGs0_3QRyT4us4Prxe8tYsMaAkugN1-3zqEaExNRGFIG4p77IBatgRYw6BJazgffM2nB8iZMubuRHyEylJdAQEC2ZqLZYeycWnqEllGJEn3gW2io6juzcCZ5vltb1-yhOTJUbzCeSVOpO5DSJqT_M66S2xDG3yYh74SwATlp4lOm9kzo3_kjhi2PXtWwcJdWGEZwD0tWO_eoc2MZ_fm6Bk2-BSX8kwfBvWqss4MDwlwlhJx6yTv-npM9tu6Qj1L3kSYfiONxHMi-qqJNdiYC7EopVAL8fpnss5VYJvuWssMm3LZwPxbUsytKNbVhxVX_DVOyq6lTH3gWKo1EQmzHqSQc_JsyCp0-WriZCvobDYgBSsGB--_ocr1YP7tcY0Oq5zlfuQgxJrrVdFS2k7tTT_493g-yH-tqTcTPqcoub2qKVs0eGXoByEwC8wZlv5Gzrh4heZJFFsTNNsHkLRvsDqjrogiNZdDSnezO9YzgZDq2RIt0lzBy3rIqNsTaQKDa2PzTBxOkHw8BYZ7IuJbUPwpCI3v4f1ZwJi9udtZUE3zTA7W7hNDwsM0jyqyPvSMREcM_iOtddV0_aWHbmqeukc-ZhXoGHsmwbfbTE2FrMrbQJt23YasiWsDFgV1rJc3lje1ZHOAuaJBPqaeiyQ6IM5QRAaQEQKmcGZord5ZOigRftaRs7EHafz8qxQvLh-GT8gRdZWUhv_dmX2FZpav0boJgouXIPZ3akTYXJD6oU66rHCrhu7fcttE-WrtSMcONfG05adhWFqn7HZqMfaA-53J6fyEnyXlMbaGqYQc454BBfmTF1NvLiME9dLL9xZWc1ZiFcb02O-n2iEqY268IS2NkzcIOVS_I420c2sPp3gTmA8i9Ox8eYV1vILufPHSSIZZvKPR96T_FBZPgA54KQV0x2-hgSygiZIFpzc6kPbCH0RbqodZKNjaffJIJI3NOEV1Ab1KMOyZLfntkD9VPckv3utND3KthYUdCDuEY8KCsznsrDk8s4cBmgVO7KW9LNZtPx6ulrLqK5M37uRPSnYUfMWcWfuXAMd6PBQZFnNcfsbEJ4-4LuHLkZ-hOy7Ys4GHthIyqEZxP-xfCrVsSwOlHQaeUQ44hnV-s72j_YsTiJI"
    
    dm = DropboxManager(TOKEN)
    
    # This will now create a CSV with columns: 
    # folder_name, file_name, size_bytes, server_modified, path_display, id
    dm.save_metadata_to_csv("/Nizar/sec_forms", "sec_forms_report.csv")
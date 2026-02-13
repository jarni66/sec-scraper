from dropbox_ops import DropboxManager
 
# 1. SETUP YOUR CREDENTIALS
# Get these from the Dropbox App Console (https://www.dropbox.com/developers/apps)
APP_KEY = "dtm7p8v46wtwjh7"
APP_SECRET = "ocp7hvlybeyoyqg"

# If you already have a refresh token from a previous run, paste it here.
# If you leave it as None, the script will open a link for you to log in.
REFRESH_TOKEN = None 

# 2. CONFIGURE SCAN SETTINGS
# Use "" for the whole Dropbox account, or "/Folder/Subfolder" for a specific path
TARGET_FOLDER = ""
OUTPUT_FILE = "dropbox_parquet_list.csv"


def refresh_parquet_list(dbx_manager, folder_path=None, output_csv=None):
    """
    Scan Dropbox for parquet files and write the list to CSV.
    Call this with an existing DropboxManager to reuse the same client.

    Args:
        dbx_manager: DropboxManager instance.
        folder_path: Dropbox path to scan ("" = whole account). Default: TARGET_FOLDER.
        output_csv: Path for the output CSV. Default: OUTPUT_FILE.

    Returns:
        List of file metadata dicts, or empty list on failure.
    """
    folder_path = folder_path if folder_path is not None else TARGET_FOLDER
    output_csv = output_csv if output_csv is not None else OUTPUT_FILE
    files = dbx_manager.get_parquet_files(
        folder_path=folder_path,
        output_csv=output_csv,
    )
    return files or []


def main():
    try:
        dbx_manager = DropboxManager(APP_KEY, APP_SECRET, REFRESH_TOKEN)
        print("\n" + "=" * 50)
        print("STARTING PARQUET INVENTORY SCAN")
        print("=" * 50 + "\n")
        files = refresh_parquet_list(dbx_manager)
        if files:
            print(f"\nSuccess! Found {len(files)} files.")
            print(f"Your CSV is ready at: {OUTPUT_FILE}")
        else:
            print("\nNo Parquet files found or an error occurred.")
    except KeyboardInterrupt:
        print("\n[STOPPED] Script stopped by user. CSV contains data found up to this point.")
    except Exception as e:
        print(f"\n[CRITICAL ERROR] {e}")


if __name__ == "__main__":
    main()
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

def main():
    try:
        # Initialize the manager (handles OAuth if token is missing)
        dbx_manager = DropboxManager(APP_KEY, APP_SECRET, REFRESH_TOKEN)

        print("\n" + "="*50)
        print("STARTING PARQUET INVENTORY SCAN")
        print("="*50 + "\n")

        # Run the incremental scan
        files = dbx_manager.get_parquet_files(
            folder_path=TARGET_FOLDER, 
            output_csv=OUTPUT_FILE
        )

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
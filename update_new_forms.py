import pandas as pd
import io
import runner_acsn
from dropbox_ops import DropboxManager
import json
# --- CONFIGURATION ---
# Use your actual keys and tokens here
APP_KEY = "dtm7p8v46wtwjh7"
APP_SECRET = "ocp7hvlybeyoyqg"
REFRESH_TOKEN = None

# 1. Initialize the Manager
dbx_manager = DropboxManager(APP_KEY, APP_SECRET, REFRESH_TOKEN)

# Load the list of already processed accessions
try:
    df_existing = pd.read_csv("dropbox_parquet_list.csv")
    df_existing['cik'] = df_existing['file_name'].map(lambda x: x.split('_')[0])
    df_existing['acsn'] = df_existing['file_name'].map(lambda x: '-'.join(x.split("_")[1:-3]))
    df_existing['report_date'] = df_existing['file_name'].map(lambda x: '-'.join(x.split("_")[4:]).split('.')[0])
    # Ensure 'acsn' is treated as string for accurate comparison
    existing_acsns = df_existing['acsn'].astype(str).unique()
except FileNotFoundError:
    print("Warning: dropbox_parquet_list.csv not found. Processing all records.")
    existing_acsns = []

def process_csv_data(df, filename):
    """
    Filters the dataframe and runs the runner_acsn logic.
    """
    print(f"--- Processing {filename} ---")
    
    if df.empty:
        print(f"File {filename} is empty.")
        return

    # Ensure accessionNumber is string to match the existing list
    df['accessionNumber'] = df['accessionNumber'].astype(str)

    # 1. Filter: Keep only those NOT in the existing acsn list
    df_filter = df[~df['accessionNumber'].isin(existing_acsns)]
    
    if df_filter.empty:
        print(f"All records in {filename} already exist. Skipping...")
        return 

    print(f"Found {len(df_filter)} new records to process.")

    # 2. Convert filtered DataFrame to list of dictionaries (records)
    records = df_filter.to_dict('records')

    # 3. Initialize and run the runner_acsn processor
    for record in records:
        try:
            # Pass the records and the dropbox manager instance
            processor = runner_acsn.ProcessACSN(record, dbx_manager)
            processor.run()
            print(f"Successfully finished runner_acsn for {filename}")
        except Exception as e:
            print(f"Error running ProcessACSN for {filename}: {e}")

def main():
    target_folder = "/Nizar/forms_table"
    
    # List files in the target folder (recursive=False for just the folder contents)
    all_files = dbx_manager.get_all_files_metadata(target_folder, recursive=False)
    
    # Filter for CSV files
    csv_files = [f for f in all_files if f['file_name'].lower().endswith('.csv')]
    
    with open(f"priority_ciks_feb.json", "r", encoding="utf-8") as f:
        priority_ciks_feb = json.load(f)


    print(f"Found {len(csv_files)} CSV files in Dropbox.\n")

    for file_info in csv_files:
        path = file_info['path_display']
        name = file_info['file_name']

        cik_ = name.split('.')[0]
        # if cik_ not in priority_ciks_feb:
        #     continue
        
        print(f"Downloading stream: {name}...")
        
        # Download stream (ensure you added this method to your DropboxManager class)
        metadata, response = dbx_manager.download_file(path)
        
        if response:
            try:
                # Read stream content into pandas
                with response:
                    df = pd.read_csv(io.BytesIO(response.content))
                    process_csv_data(df, name)
            except Exception as e:
                print(f"Error reading CSV content for {name}: {e}")
        else:
            print(f"Failed to get stream for {name}")

if __name__ == "__main__":
    main()
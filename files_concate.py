import pandas as pd
import os

input_base = "sec_forms"
output_dir = "filling_master"
os.makedirs(output_dir, exist_ok=True)

CHUNK_SIZE = 5_000_000  # adjust based on memory
file_count = 0

buffer = []   # store dataframes temporarily
buffer_rows = 0

for date_dir in os.listdir(input_base):
    folder_path = os.path.join(input_base, date_dir)

    # skip non-folders
    if not os.path.isdir(folder_path):
        continue

    for item in os.listdir(folder_path):
        file_path = os.path.join(folder_path, item)
        print("Reading:", file_path)
        try:
            df = pd.read_parquet(file_path)
            buffer.append(df)
            buffer_rows += len(df)
        except Exception as e:
            print("Error :",e)

        # When chunk limit reached -> write out
        if buffer_rows >= CHUNK_SIZE:
            combined = pd.concat(buffer, ignore_index=True)
            output_file = os.path.join(output_dir, f"filling_part_{file_count}.parquet")
            combined.to_parquet(output_file)
            print(f"[SAVED] {output_file} ({len(combined):,} rows)")

            # reset buffer
            buffer = []
            buffer_rows = 0
            file_count += 1

# Save remaining rows if any
if buffer_rows > 0:
    combined = pd.concat(buffer, ignore_index=True)
    output_file = os.path.join(output_dir, f"filling_part_{file_count}.parquet")
    combined.to_parquet(output_file)
    print(f"[SAVED REMAINDER] {output_file} ({len(combined):,} rows)")

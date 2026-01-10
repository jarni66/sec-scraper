import re
import json
import os

def parse_cik_file(input_filename, chunk_size=100000):
    # REGEX EXPLANATION:
    # ^(.*)    : Start of line, capture everything (greedy) into Group 1 (Name)
    # :        : Look for a colon separator
    # (\d+)    : Capture one or more digits into Group 2 (CIK)
    # :?       : Look for an optional trailing colon
    # $        : End of line
    regex_pattern = re.compile(r"^(.*):(\d+):?$")

    current_chunk = []
    chunk_count = 1
    total_processed = 0

    if not os.path.exists(input_filename):
        print(f"Error: {input_filename} not found.")
        return

    print(f"Starting processing: {input_filename}...")

    with open(input_filename, 'r', encoding='utf-8', errors='ignore') as file:
        for line in file:
            line = line.strip()
            if not line:
                continue

            match = regex_pattern.search(line)
            if match:
                name = match.group(1).strip()
                cik = match.group(2)
                
                current_chunk.append({
                    "name": name,
                    "cik": cik
                })

            # When chunk reaches 100k, save to file
            if len(current_chunk) >= chunk_size:
                save_json_chunk(current_chunk, chunk_count)
                total_processed += len(current_chunk)
                current_chunk = [] # Reset list
                chunk_count += 1

        # Save any remaining records in the last chunk
        if current_chunk:
            save_json_chunk(current_chunk, chunk_count)
            total_processed += len(current_chunk)

    print(f"Finished! Processed {total_processed} records into {chunk_count} files.")

def save_json_chunk(data, index):
    output_filename = f"cik_chunks/cik_data_part_{index}.json"
    with open(output_filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4)
    print(f"Saved {output_filename} ({len(data)} records)")

if __name__ == "__main__":
    # Ensure your text file is in the same folder or provide the full path
    parse_cik_file('cik-lookup-data.txt')
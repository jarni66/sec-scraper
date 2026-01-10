import subprocess
import os
import time

def launch_workers(start_range, end_range):
    # The name of the script you want to run
    script_to_run = "process_cik_chunk.py"
    
    # Path to the folder containing your chunks
    chunk_folder = "cik_chunks"

    print(f"Checking for files in range {start_range} to {end_range}...")

    for i in range(start_range, end_range + 1):
        chunk_file = f"{chunk_folder}/cik_data_part_{i}.json"
        
        # Verify the file exists before trying to open a window
        if not os.path.exists(chunk_file):
            print(f"Skipping: {chunk_file} not found.")
            continue

        print(f"Launching PowerShell for chunk {i}...")

        # Construct the command:
        # 'start powershell' opens a new window
        # '-NoExit' keeps the window open after the script finishes so you can see results
        # 'python ...' executes your processing script
        command = f'start powershell -NoExit -Command "python {script_to_run} {chunk_file}"'

        # Execute the command
        subprocess.Popen(command, shell=True)

        # Wait 3 seconds before starting the next one.
        # This prevents 11 windows from hitting the SEC server at the exact same millisecond.
        time.sleep(3)

if __name__ == "__main__":
    # --- CONFIGURATION ---
    START_PART = 2
    END_PART = 11
    # ---------------------

    launch_workers(START_PART, END_PART)
    print("\nAll windows have been requested. Check your taskbar for the new PowerShell windows.")
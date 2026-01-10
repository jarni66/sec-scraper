import subprocess
import os

# --- CONFIGURATION ---
START_CHUNK = 11
END_CHUNK = 20
CHUNK_FOLDER = "pending_chunks"
SCRIPT_TO_RUN = "process_chunk.py"  # Ensure this matches your filename

def main():
    print(f"--- Spawning PowerShell windows for chunks {START_CHUNK} to {END_CHUNK} ---")

    for i in range(START_CHUNK, END_CHUNK + 1):
        chunk_name = f"upload_chunk_{i}.csv"
        chunk_path = os.path.join(CHUNK_FOLDER, chunk_name)

        # Check if the chunk file actually exists before trying to run it
        if not os.path.exists(chunk_path):
            print(f"Warning: {chunk_path} not found. Skipping...")
            continue

        # Build the command
        # 'powershell -NoExit' keeps the window open so you can see the summary at the end
        command = f'python {SCRIPT_TO_RUN} {chunk_path}'
        ps_full_command = f'powershell -NoExit -Command "{command}"'

        print(f"Launching window for Chunk {i}...")

        # CREATE_NEW_CONSOLE tells Windows to open a separate window for each process
        subprocess.Popen(
            ps_full_command,
            creationflags=subprocess.CREATE_NEW_CONSOLE
        )

    print("\nAll windows have been launched.")

if __name__ == "__main__":
    main()
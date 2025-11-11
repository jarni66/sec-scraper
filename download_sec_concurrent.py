from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from update_gdrive import build_service, list_items_in_folder, download_file, sec_folder_id

def download_sec_concurrent(max_workers=20):
    service = build_service()
    drive_folder_list = list_items_in_folder(service, sec_folder_id)

    download_tasks = []

    # Collect download tasks
    for drf in drive_folder_list:
        folder_name = drf.get('name')
        folder_id = drf.get('id')

        local_folder_path = f"sec_forms/{folder_name}"
        os.makedirs(local_folder_path, exist_ok=True)

        item_list = list_items_in_folder(service, folder_id)
        local_files = set(os.listdir(local_folder_path))

        for drive_file in item_list:
            if drive_file.get('mimeType') == 'application/vnd.google-apps.folder':
                continue

            file_name = drive_file.get('name')
            if file_name not in local_files:
                download_tasks.append({
                    "file_id": drive_file.get('id'),
                    "local_path": f"{local_folder_path}/{file_name}"
                })

    print(f"Found {len(download_tasks)} files to download.")
    with open("log/concurrent_download.txt", "w", encoding="utf-8") as f:
        f.write(f"{len(download_tasks)} files to download.")
    # Download concurrently
    successful = 0
    failed = 0

    def _download(task):
        file_id = task["file_id"]
        local_path = task["local_path"]
        result = download_file(file_id, local_path)
        return result, local_path

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(_download, t) for t in download_tasks]

        for future in as_completed(futures):
            result, local_path = future.result()
            if result:
                successful += 1
                print("Downloaded:", local_path)
            else:
                failed += 1
                print("Failed:", local_path)

    print(f"\nDownload complete: {successful} success, {failed} failed")


if __name__ == "__main__":
    download_sec_concurrent(max_workers=20)

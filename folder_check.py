from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from google.auth import default
import os
import io
import csv
import pandas as pd
import json

SCOPES = [
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/drive",
]

sec_folder_id = "1bjX-ozr5VDjsEsee8wr-mTow-u0iyr7_"
raw_txt_folder_id = "1JrP2wOw6-K5646kN8aP9iRo36OgCgG3v"

def build_service():
    creds, _ = default(scopes=SCOPES)
    return build("drive", "v3", credentials=creds)



def list_items_in_folder(service, folder_id):
    items = []
    page_token = None

    # includeItemsFromAllDrives/supportsAllDrives=True helps if the folder is
    # in a Shared Drive (it’s harmless for My Drive)
    while True:
        resp = service.files().list(
            q=f"'{folder_id}' in parents and trashed=false",
            fields="nextPageToken, files(id, name, mimeType, modifiedTime, size, owners(displayName,emailAddress))",
            pageSize=1000,
            pageToken=page_token,
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
        ).execute()
        items.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return items


def chcek_folder():
    service = build_service()
    """Download files from Google Drive sec_forms folder to local sec_forms directory"""
    drive_folder_list = list_items_in_folder(service, sec_folder_id)
    with open("chcek_folder.json", "w", encoding="utf-8") as f:
        json.dump(drive_folder_list, f, indent=4)
    print("Checking Drive Folders")

def delete_folder_direct(folder_id: str):
    """
    Delete a Google Drive folder directly (moves it to Trash).
    This also affects all files and subfolders inside it.
    """
    service = build_service()
    service.files().delete(fileId=folder_id, supportsAllDrives=True).execute()
    print(f"🗑️ Folder {folder_id} moved to Trash (including all contents)")


def del_folders():
    with open("todel_folder.json", "r", encoding="utf-8") as f:
        safe_folder = json.load(f)

    with open("chcek_folder.json", "r", encoding="utf-8") as f:
        chcek_folder = json.load(f)

    for fol in chcek_folder:
        fol_name = fol.get('name')
        if fol_name in safe_folder:
            delete_folder_direct(fol.get('id'))
    


del_folders()
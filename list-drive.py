#!/usr/bin/env python3

import argparse
import os
import sys
import time

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

# Scopes for read-only access to Google Drive
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

def authenticate():
    """Authenticate the user and return the Drive API service."""
    creds = None
    if os.path.exists('token-drive.json'):
        creds = Credentials.from_authorized_user_file('token-drive.json', SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'client_secret.json', SCOPES
            )
            creds = flow.run_local_server(port=0)
        with open('token-drive.json', 'w') as token:
            token.write(creds.to_json())
    return build('drive', 'v3', credentials=creds)

def get_folder_id_by_name(service, folder_name):
    """Fetch the folder ID by its name."""
    try:
        results = service.files().list(
            q=f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed = false",
            fields="files(id, name)",
            pageSize=1
        ).execute()

        items = results.get('files', [])
        if not items:
            print(f"Folder '{folder_name}' not found.", file=sys.stderr)
            sys.exit(1)
        return items[0]['id']
    except HttpError as error:
        print(f"An error occurred: {error}", file=sys.stderr)
        sys.exit(1)

def fetch_with_retries(service, query, fields, page_token=None, retries=5):
    """Fetch data from the API with retry logic for transient errors."""
    for attempt in range(retries):
        try:
            return service.files().list(
                q=query,
                fields=fields,
                pageSize=1000,
                pageToken=page_token
            ).execute()
        except HttpError as error:
            if error.resp.status in [429, 500, 503]:
                print(f"Transient error {error.resp.status}: {error}. Retrying in 1 second...", file=sys.stderr)
                time.sleep(1)
            else:
                print(f"Non-retryable error: {error}.", file=sys.stderr)
                raise
    print("Max retries reached for a list call.", file=sys.stderr)
    raise HttpError("Retries exhausted.")

def process_item(service, item, parent_id,  visited_folders):
    """Process a single item and write its details to TSV."""
    # print(item, file=sys.stderr)
    try:
        createdTime =  item.get('createdTime', '')
        size = item.get('size', '')  # Size is not available for folders
        quotaBytesUsed = item.get('quotaBytesUsed', '')
        spaces = item.get('spaces', '')
        print(f"{spaces}\t{createdTime}\t{quotaBytesUsed}\t{size}\t{item['id']}\t{parent_id}\t{item['name']}")
        if item['mimeType'] == 'application/vnd.google-apps.folder':
            list_files_recursive(service, item['id'], visited_folders)
    except HttpError as sub_error:
        print(f"Error processing item '{item['name']}' ({item['id']}): {sub_error}", file=sys.stderr)
        print(f"Skipping item '{item['name']}' due to unrecoverable error.", file=sys.stderr)

def list_files_recursive(service, folder_id, visited_folders=None):
    """Recursively list files in Google Drive and write their details to TSV."""
    if visited_folders is None:
        visited_folders = set()

    # Check for circular references
    if folder_id in visited_folders:
        print(f"Skipping already visited folder ID: {folder_id}", file=sys.stderr)
        return

    # Mark the folder as visited
    visited_folders.add(folder_id)

    print(f"processing folder ID: {folder_id}", file=sys.stderr)
    

    query = f"'{folder_id}' in parents and trashed = false"
    fields = "nextPageToken, files(*)"  #  files(id, name, mimeType, size, parents, createdTime, quotaBytesUsed)"

    try:
        next_page_token = None
        while True:
            results = fetch_with_retries(service, query, fields, next_page_token)
            items = results.get('files', [])
            for item in items:
                process_item(service, item, folder_id, visited_folders)

            next_page_token = results.get('nextPageToken')
            if not next_page_token:
                break
    except HttpError as error:
        print(f"Unrecoverable error listing folder ID '{folder_id}': {error}", file=sys.stderr)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Recursively list files in Google Drive.")
    parser.add_argument('-i', '--id', help="Starting folder ID")
    parser.add_argument('-n', '--name', help="Starting top-level folder name")
    args = parser.parse_args()

    service = authenticate()

    if args.id:
        folder_id = args.id
        print(f"Using provided folder ID: {folder_id}", file=sys.stderr)
    elif args.name:
        print(f"Searching for folder: {args.name}", file=sys.stderr)
        folder_id = get_folder_id_by_name(service, args.name)
    else:
        print("No folder ID or name provided. Starting from the root folder.", file=sys.stderr)
        folder_id = 'root'

    print('Spaces\tCreatedTime\tQuotaBytesUsed\tSize\tID\tParent Folder ID\tName')

    print(f"Listing files starting from folder: {args.name if args.name else folder_id}", file=sys.stderr)
    list_files_recursive(service, folder_id)

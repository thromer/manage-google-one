#!/usr/bin/env python3

import os
import requests
import sys
import time

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

from pprint import pprint


# Scopes for read-only access to Google Photos
SCOPES = ['https://www.googleapis.com/auth/photoslibrary.readonly']

def authenticate():
    """Authenticate the user and return valid credentials."""
    creds = None
    # Check if token-photos.json exists to use previous authentication
    if os.path.exists('token-photos.json'):
        creds = Credentials.from_authorized_user_file('token-photos.json', SCOPES)
    
    # If there are no valid credentials, authenticate using the OAuth flow
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'client_secret.json', SCOPES
            )
            creds = flow.run_local_server(port=0)
        # Save the credentials for future use
        with open('token-photos.json', 'w') as token:
            token.write(creds.to_json())
    return creds

def fetch_with_retries(url, headers, params, retries=5):
    """Fetch data from the API with retry logic for transient errors."""
    for attempt in range(retries):
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            return response
        elif response.status_code in [429, 500, 503]:
            print(f"Transient error {response.status_code}: {response.text}. Retrying in 1 second...", file=sys.stderr)
            time.sleep(1)
        else:
            print(f"Non-retryable error {response.status_code}: {response.text}.", file=sys.stderr)
            return response
    print("Max retries reached. Exiting.", file=sys.stderr)
    return None

def list_google_photos(creds):
    """Fetch and list all photos with pagination support."""
    headers = {'Authorization': f'Bearer {creds.token}'}
    url = 'https://photoslibrary.googleapis.com/v1/mediaItems'
    params = {'pageSize': 100}

    while True:
        # Fetch data with retry logic
        # print(f"{params=}", file=sys.stderr)
        response = fetch_with_retries(url, headers, params)
        if not response:
            print("Failed to fetch data. Exiting.", file=sys.stderr)
            break

        data = response.json()

        # List all media items from the current page
        if 'mediaItems' in data:
            for item in data['mediaItems']:
                filename = item.get('filename', '')
                mediaMetadata = item.get('mediaMetadata', {})
                # size = mediaMetadata.get('fileSize', '')
                creationTime = mediaMetadata.get('creationTime', '')
                # base_url = item.get('baseUrl', '')
                print(f"{creationTime}\t{filename}")
        else:
            print(f"No media items found in {data}, quitting (?)", file=sys.stderr)
            break

        # Check if there's a next page
        next_page_token = data.get('nextPageToken')
        if not next_page_token:
            break

        # Set the nextPageToken for the next request
        params['pageToken'] = next_page_token

def list_google_albums(service):
    nextPageToken = None
    while True:
        result = service.albums().list(pageSize=50, pageToken=nextPageToken).execute()
        pprint(result.get('albums', []))
        nextPageToken = result.get('nextPageToken', None)
        if not nextPageToken:
            break


def search(service, filters=None):
    nextPageToken = None
    print('\t'.join([
        'mimeType',
        'creationTime',
        'id',
        'mediaItemsCount',
        'title',
        'filename'
    ]))
    while True:
        body = {
            "pageSize": 100,
            "pageToken": nextPageToken,
            "filters": filters
        }
        result = service.mediaItems().search(body=body).execute()
        # pprint(result.get('mediaItems', []))
        for item in result.get('mediaItems', []):
            pprint(item, stream=sys.stderr)
            print('\t'.join([
                item.get('mimeType', ''),
                item.get('mediaMetadata', {}).get('creationTime', ''),
                item.get('id', ''),
                item.get('mediaItemsCount', ''),
                item.get('title', ''),
                item.get('filename', '')
            ]))
        nextPageToken = result.get('nextPageToken', None)
        if not nextPageToken:
            break


if __name__ == '__main__':
    creds = authenticate()
    service = build('photoslibrary', 'v1', credentials=creds, static_discovery=False)
    single_date_filters = {
        "dateFilter": {
            "dates": [
                {
                    "year": 2013,
                    "month": 3,
                    "day": 16
                }
            ]
        }
    }
    search(service, filters=single_date_filters)
    # list_google_albums(service)
    # list_google_photos(creds)

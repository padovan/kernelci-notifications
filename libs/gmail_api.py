#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# Define the required Gmail API scope
SCOPES = ['https://mail.google.com/']
CREDENTIALS_FILE = '/home/gus/Downloads/client_secret_415818378487-sj560uksa25vpn1v8a2hh5jsrft02rqk.apps.googleusercontent.com.json'  # Replace with the path to your OAuth credentials file
TOKEN_FILE = 'token.json'  # File to store the access and refresh tokens

def gmail_setup_service():
    """Authenticate with Gmail using OAuth2 and get credentials."""
    creds = None

    # Load tokens if available
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)

    # If there are no valid credentials, initiate a new OAuth flow
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)

        # Save the credentials for future use
        with open(TOKEN_FILE, 'w') as token:
            token.write(creds.to_json())

    service = build('gmail', 'v1', credentials=creds)
    return service

def gmail_send_email(service, user_id, email, dry_run=False):
    """Send an email using the Gmail API."""
    if dry_run:
        print(email)
        return "<dry run>"

    try:
        sent_message = service.users().messages().send(userId=user_id, body=email).execute()
        print(f"Message sent successfully! Message ID: {sent_message['id']}")
        return sent_message['id']
    except Exception as error:
        print(f"An error occurred: {error}")


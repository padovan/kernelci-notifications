#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import base64
import os

from email.message import EmailMessage
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

def gmail_send_email(service, user_id, email):
    """Send an email using the Gmail API."""

    try:
        sent_message = service.users().messages().send(userId=user_id, body=email).execute()

        full_message = service.users().messages().get(
            userId=user_id,
            id=sent_message['id'],
            format='full'
        ).execute()

        headers = full_message['payload']['headers']
        message_id = next(
            (header['value'] for header in headers if header['name'].lower() == 'message-id'),
            None
        )
        print(f"Message sent successfully! Message ID: {message_id}")

        return message_id

    except Exception as error:
        print(f"An error occurred: {error}")


def create_email(sender, to, subject, message_text, cc):
    message = EmailMessage()
    message.set_content(message_text)
    # Set email headers
    if to:
        message['to'] = to
    if cc:
        message['cc'] = cc
    message['from'] = sender
    message['subject'] = subject

    # Encode the message as base64
    raw = base64.urlsafe_b64encode(message.as_bytes()).decode()
    return {'raw': raw}


def ask_confirmation():
    while True:
        choice = input(">> Do you want to send the email? (y/n): ").strip().lower()
        if choice in ["y", "yes"]:
            return True
        elif choice in ["n", "no"]:
            return False
        else:
            print("Please enter 'y' or 'n'.")


def send_email_report(service, report, recipients, email_args):
    sender_email = "KernelCI bot <bot@kernelci.org>"
    subject = report["title"]
    message_text = report["content"]

    if not email_args.send:
        print("\n==============================================")
        print(f"new report:\n> {subject}")
        print(message_text)
        return None

    print(email_args.ignore_recipients)
    if not email_args.ignore_recipients:
        cc = recipients
    else:
        cc = ""

    if email_args.cc:
        cc = ', '.join([email_args.cc, cc])

    if not email_args.yes:
        print("===================")
        print(f"Subject: {subject}")
        print(f"To: {email_args.to}")
        if cc:
            print(f"Cc: {cc}")
        print(message_text)
        if not ask_confirmation():
            print("Email sending aborted.")
            return None


    print(f"sending {subject}.")

    email = create_email(sender_email, email_args.to, subject, message_text, cc)
    return gmail_send_email(service, 'me', email)

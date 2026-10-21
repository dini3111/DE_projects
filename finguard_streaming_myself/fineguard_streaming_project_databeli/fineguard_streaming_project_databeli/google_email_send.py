# Databricks notebook source
email_body = """Hello Team,

This is a dynamic report body. You can update this string based on your logic:
* Status: Success
* Processed Rows: 15,240
* Execution Mode: Serverless Free Edition

Regards,
Databricks Automation Bot"""
# ==========================================

# COMMAND ----------

import json
import base64
import requests
from email.mime.text import MIMEText

# ==========================================
# DYNAMIC EMAIL CONFIGURATION
# ==========================================
recipient_email = "reddydinesh3111@gmail.com"
sender_email = "reddydinesh3111@gmail.com"        # <-- FIXED: Removed double @@ typo
email_subject = "Automated Databricks Serverless Report"

email_body = """Hello Team,

This is a dynamic report body executing successfully from your workspace.
* Status: Success
* Compute Layer: Serverless Free Edition

Regards,
Databricks Automation Bot"""
# ==========================================


# 1. RETRIEVE THE OAUTH PAYLOAD SECURELY FROM YOUR VAULT
secret_string = dbutils.secrets.get(scope="finguard-scope", key="gmail_oauth_credentials")
config = json.loads(secret_string)

# 2. GENERATE A FRESH ACCESS TOKEN VIA STANDARDIZED API ENDPOINT
token_url = "https://oauth2.googleapis.com/token"  # Google OAuth2 token endpoint

token_data = {
    "client_id": config["client_id"],
    "client_secret": config["client_secret"],
    "refresh_token": config["refresh_token"],
    "grant_type": "refresh_token"
}

headers_token = {
    "Content-Type": "application/x-www-form-urlencoded"
}

# Sending as application/x-www-form-urlencoded data payload
token_response = requests.post(token_url, data=token_data, headers=headers_token)

if token_response.status_code != 200:
    raise Exception(f"Failed to refresh Google token: {token_response.status_code} - {token_response.text}")

access_token = token_response.json()["access_token"]

# 3. CONSTRUCT THE EMAIL MESSAGE OBJECT
raw_message = MIMEText(email_body, "plain")
raw_message['to'] = recipient_email
raw_message['from'] = sender_email
raw_message['subject'] = email_subject

# Encode format for Gmail REST API architecture
raw_string = base64.urlsafe_b64encode(raw_message.as_bytes()).decode()
payload = {'raw': raw_string}

# 4. DISPATCH THROUGH GMAIL REST API
send_url = "https://gmail.googleapis.com/gmail/v1/users/me/messages/send"  # Gmail API send endpoint
headers_email = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json"
}

email_response = requests.post(send_url, json=payload, headers=headers_email)

if email_response.status_code == 200:
    print("Email sent successfully and securely using zero custom libraries!")
else:
    print(f"Error sending email: {email_response.status_code} - {email_response.text}")

from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.dataframe import DataFrame
import json
import base64
import requests
from email.mime.text import MIMEText

# ==========================================
# EMAIL CONFIGURATION
# ==========================================
recipient_email = "reddydinesh3111@gmail.com"
sender_email = "reddydinesh3111@gmail.com"

# Retrieve credentials at module level (outside handler for compatibility)
_secret_string = dbutils.secrets.get(scope="finguard-scope", key="gmail_oauth_credentials")
_oauth_config = json.loads(_secret_string)


def _send_notification(subject, body):
    """
    Send notification via Gmail REST API.
    Adapted from the google_email_send notebook (Cell 2) - tested and confirmed working.
    """
    # 1. Refresh the OAuth2 access token
    token_url = "https://oauth2.googleapis.com/token"
    token_data = {
        "client_id": _oauth_config["client_id"],
        "client_secret": _oauth_config["client_secret"],
        "refresh_token": _oauth_config["refresh_token"],
        "grant_type": "refresh_token",
    }
    headers_token = {"Content-Type": "application/x-www-form-urlencoded"}
    token_response = requests.post(token_url, data=token_data, headers=headers_token)
    if token_response.status_code != 200:
        raise Exception(
            f"Failed to refresh Google token: {token_response.status_code} - {token_response.text}"
        )
    access_token = token_response.json()["access_token"]

    # 2. Build the email message
    raw_message = MIMEText(body, "plain")
    raw_message["to"] = recipient_email
    raw_message["from"] = sender_email
    raw_message["subject"] = subject
    raw_string = base64.urlsafe_b64encode(raw_message.as_bytes()).decode()
    payload = {"raw": raw_string}

    # 3. Dispatch via Gmail REST API
    send_url = "https://gmail.googleapis.com/gmail/v1/users/me/messages/send"
    headers_email = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    email_response = requests.post(send_url, json=payload, headers=headers_email)
    if email_response.status_code == 200:
        print(f"Alert email sent: {subject}")
    else:
        print(f"Error sending alert email: {email_response.status_code} - {email_response.text}")


@dp.foreach_batch_sink(name="high_transaction_alert_notification_sink")
def alert_notification_sink(df, batch_id):
    """
    ForEachBatch sink that sends notification for each new
    high-value transaction alert written to the gold alert table.
    """
    # Use collect() to iterate rows (toLocalIterator() is not supported
    # when file-based collect is enabled on this compute)
    alert_count = 0
    for alert in df.collect():
        subject = f"[FineGuard Alert] {alert.alert_type} - {alert.alert_id}"

        body = f"""High-Value Transaction Alert

Alert Details:
  Alert ID      : {alert.alert_id}
  Alert Type    : {alert.alert_type}
  Alert Time    : {alert.alert_timestamp}

Customer:
  Name          : {alert.customer_name}
  Customer ID   : {alert.customer_id}
  Email         : {alert.customer_email_address}

Transaction:
  Transaction ID: {alert.transaction_id}
  Amount        : {alert.transaction_amount} {alert.currency}
  Customer Limit: {alert.transaction_limit} {alert.currency}
  Merchant      : {alert.merchant_name} ({alert.merchant_category})
  Type          : {alert.transaction_type}
  Channel       : {alert.payment_channel}
  Location      : {alert.city}, {alert.country}
  International : {alert.is_international}
  Timestamp     : {alert.transaction_timestamp}
  Status        : {alert.status}

This is an automated alert from the FineGuard Fraud Monitoring System.
"""

        _send_notification(subject, body)
        alert_count += 1
    print(f"Batch {batch_id}: Sent {alert_count} fraud alerts via email.")


@dp.append_flow(target="high_transaction_alert_notification_sink")
def alert_notification_flow():
    """
    Streaming flow that reads new high-value transaction alerts
    from the gold table and triggers notifications.
    """
    return spark.readStream.table("finguard.gold.customer_high_transaction_alert")

import os.path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import base64
from email.message import EmailMessage
import cred
import pandas as pd

# Define scope
SCOPES = ["https://www.googleapis.com/auth/gmail.send"]

def connect_to_gmail_api(scopes):
  # get credentials from JSON access token
  if os.path.exists("token.json"):
    creds = Credentials.from_authorized_user_file("token.json", scopes)
  if not creds or not creds.valid:
    # refresh JSON token if necessary
    if creds and creds.expired and creds.refresh_token:
      creds.refresh(Request())
    # create JSON token if it does not exist
    else:
      flow = InstalledAppFlow.from_client_secrets_file(
          "credentials.json", SCOPES
      )
      creds = flow.run_local_server(port=0)
    # save the credentials for the next run
    with open("token.json", "w") as token:
      token.write(creds.to_json())
    
  return creds
  
def send_email(creds, top_songs_df, top_artists_df):
  try:
    # connect to gmail API
    service = build("gmail", "v1", credentials=creds)
    message = EmailMessage()

    # set message fields
    message.set_content(f"Your top 5 artists this week were {[top_artists_df.iloc[i]['Artist'] for i in range(len(top_artists_df))]} and your top 5 songs were  {[top_songs_df.iloc[i]['Song'] for i in range(len(top_songs_df))]}")
    message["To"] = cred.receiver_email
    message["From"] = cred.sender_email
    message["Subject"] = "Your Weekly Spotify Wrapped :)"

    # encoded message
    encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()

    # send message
    create_message = {"raw": encoded_message}
    send_message = (
        service.users()
        .messages()
        .send(userId="me", body=create_message)
        .execute()
    )
    print(f'Message Id: {send_message["id"]}')

  except HttpError as error:
    # print error message if any (hopefully not)
    print(f"An error occurred: {error}")

if __name__ == "__main__":
  # creds = connect_to_gmail_api(SCOPES)
  # send_email(creds, top_songs, top_artists)
    pass
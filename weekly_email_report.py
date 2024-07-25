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
from write_data_to_postgres import connect_to_postgres

# Define scope
SCOPES = ["https://www.googleapis.com/auth/gmail.send"]

def connect_to_gmail_api(scopes):
  # get credentials from JSON access token
  if os.path.exists("/Users/christophercorallo/projects/spotify_data_pipeline/token.json"):
    creds = Credentials.from_authorized_user_file("token.json", scopes)
  else:
    creds = None
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
  
def send_email(creds, top_songs, top_artists):
  try:
    # connect to gmail API
    service = build("gmail", "v1", credentials=creds)
    message = EmailMessage()

    # set message fields
    message.set_content(f"""
                        ARTISTS
                        1. {top_artists[0][0]} with {top_artists[0][1]} songs played
                        2. {top_artists[1][0]} with {top_artists[1][1]} songs played
                        3. {top_artists[2][0]} with {top_artists[2][1]} songs played
                        4. {top_artists[3][0]} with {top_artists[3][1]} songs played
                        5. {top_artists[4][0]} with {top_artists[4][1]} songs played

                        SONGS
                        1. {top_songs[0][0]} with {top_songs[0][1]} listens
                        2. {top_songs[1][0]} with {top_songs[1][1]} listens
                        3. {top_songs[2][0]} with {top_songs[2][1]} listens
                        4. {top_songs[3][0]} with {top_songs[3][1]} listens
                        5. {top_songs[4][0]} with {top_songs[4][1]} listens
                        """)
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

def fetch_top_weekly(con):
  # initialize cursor
  cur = con.cursor()

  song_query = "with last_weeks_songs as (select distinct \"SONG_NAME\", \"ARTIST_NAME\", TO_TIMESTAMP(\"PLAYED_AT\",'YYYY-MM-DDTHH24:MI:SS.MSZ') as play_time from spotify_user_data.\"recently_played\") select \"SONG_NAME\", count(\"SONG_NAME\") as total from last_weeks_songs WHERE play_time >= current_date - interval '7 days' group by 1 order by total desc limit 5;"  
  artist_query = "with last_weeks_artists as (select distinct \"SONG_NAME\", \"ARTIST_NAME\", TO_TIMESTAMP(\"PLAYED_AT\",'YYYY-MM-DDTHH24:MI:SS.MSZ') as play_time from spotify_user_data.\"recently_played\") select \"ARTIST_NAME\", count(\"ARTIST_NAME\") as total from last_weeks_artists WHERE play_time >= current_date - interval '7 days' group by 1 order by total desc limit 5;"
  
  try:
    cur.execute(artist_query)
    top_artists = cur.fetchall()
    cur.execute(song_query)
    top_songs = cur.fetchall()
  finally:
    cur.close()
  
  return top_songs, top_artists


connection = connect_to_postgres()
top_songs, top_artists = fetch_top_weekly(connection)
creds = connect_to_gmail_api(SCOPES)
send_email(creds, top_songs, top_artists)
print("HOORAY!!!")
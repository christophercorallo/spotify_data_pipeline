import spotipy
from spotipy.oauth2 import SpotifyOAuth
import cred
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import datetime

def run_spotify_etl():

    scope = 'user-read-recently-played'

    # authenticate api
    sp = spotipy.Spotify(
        auth_manager=SpotifyOAuth(client_id=cred.client_id, client_secret=cred.client_secret, redirect_uri=cred.redirect_url, scope=scope)
        )

    song_names = []
    artist_names = []
    played_at_list = []
    date_timestamps = []

    # retrieve data from api
    results = sp.current_user_recently_played()

    # add data to lists
    for song in results['items']:
        song_names.append(song['track']['name'])
        artist_names.append(song['track']['artists'][0]['name'])
        played_at_list.append(song['played_at'])
        date_timestamps.append(song['played_at'][0:10])

    song_dict = {
        'SONG_NAME': song_names,
        'ARTIST_NAME': artist_names,
        'PLAYED_AT': played_at_list,
    }

    # create dataframe
    song_df = pd.DataFrame(song_dict, columns=['SONG_NAME','ARTIST_NAME','PLAYED_AT'])



    # write data to snowflake database

    # connect to snowflake
    con = snowflake.connector.connect(
        user = cred.sf_username,
        password = cred.sf_password,
        account = cred.sf_account,
        warehouse = cred.sf_warehouse,
        database = cred.sf_database,
        schema = cred.sf_schema
        )

    try:
        # query date of latest record
        cur = con.cursor()
        most_recent = cur.execute(
            ('select played_at '
            'from SPOTIFY_DATA.SPOTIFY_RECENTLY_PLAYED.SPOTIFY_RECENTLY_PLAYED '
            'order by played_at desc '
            'limit 1;')
            ).fetchone()[0]
            
        # remove rows from dataframe after most_recent from query
        for id, date in enumerate(song_df['PLAYED_AT']):
            current_date = datetime.datetime.strptime(date, '%Y-%m-%dT%H:%M:%S.%fZ')
            if current_date < most_recent:
                song_df.drop(id, inplace = True)
                
        # add data to database
        success, nchunks, nrows, _ = write_pandas(con, song_df, 'SPOTIFY_RECENTLY_PLAYED')
    finally:
        # close connection
        con.close()

run_spotify_etl()
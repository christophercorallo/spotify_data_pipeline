import spotipy
from spotipy.oauth2 import SpotifyOAuth
import cred
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import datetime

def connect_to_spotify(scope):
    # authenticate api
    sp = spotipy.Spotify(
        auth_manager=SpotifyOAuth(client_id=cred.client_id, client_secret=cred.client_secret, redirect_uri=cred.redirect_url, scope=scope)
        )
    # retrieve data from api
    results = sp.current_user_recently_played()  
    return results

def load_songs_to_df(songs):
    # define lists
    song_names = []
    artist_names = []
    played_at_list = []

    # add data to lists
    for song in songs['items']:
        song_names.append(song['track']['name'])
        artist_names.append(song['track']['artists'][0]['name'])
        played_at_list.append(song['played_at'])

    song_dict = {
        'SONG_NAME': song_names,
        'ARTIST_NAME': artist_names,
        'PLAYED_AT': played_at_list,
    }

    # create dataframe
    song_df = pd.DataFrame(song_dict, columns=['SONG_NAME','ARTIST_NAME','PLAYED_AT'])

    return song_df

def write_data_to_snowflake(song_df):
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

recent_songs = connect_to_spotify('user-read-recently-played')
recent_songs_df = load_songs_to_df(recent_songs)
write_data_to_snowflake(recent_songs_df)
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import cred
import pandas as pd
from sqlalchemy import create_engine

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
        song_names.insert(0,song['track']['name'])
        artist_names.insert(0,song['track']['artists'][0]['name'])
        played_at_list.insert(0,song['played_at'])

    song_dict = {
        'SONG_NAME': song_names,
        'ARTIST_NAME': artist_names,
        'PLAYED_AT': played_at_list,
    }

    # create dataframe
    song_df = pd.DataFrame(song_dict, columns=['SONG_NAME','ARTIST_NAME','PLAYED_AT'])

    return song_df

def write_data_to_postgres(song_df):
    # connect to postgres
    con = create_engine(cred.postgres_url)
    # write songs to postgres
    song_df.to_sql('recently_played', con, schema="spotify_user_data",if_exists="append", index=False)

recent_songs = connect_to_spotify('user-read-recently-played')
recent_songs_df = load_songs_to_df(recent_songs)
write_data_to_postgres(recent_songs_df)


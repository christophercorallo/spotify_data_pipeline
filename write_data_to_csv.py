import spotipy
from spotipy.oauth2 import SpotifyOAuth
import cred
import pandas as pd
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

def load_df_to_csv(df):
    last_csv_row = pd.read_csv('spotify_listening_data.csv').iloc[-1:,:].values
    if last_csv_row.any():
        most_recent = datetime.datetime.strptime(last_csv_row[0,3], '%Y-%m-%dT%H:%M:%S.%fZ')

        for id, date in enumerate(df['PLAYED_AT']):
            current_date = datetime.datetime.strptime(date, '%Y-%m-%dT%H:%M:%S.%fZ')
            if current_date <= most_recent:
                df.drop(id, inplace = True)

    df.to_csv('spotify_listening_data.csv', mode='a', header=False)

if __name__ == "__main__":
    recent_songs = connect_to_spotify('user-read-recently-played')
    recent_songs_df = load_songs_to_df(recent_songs)
    load_df_to_csv(recent_songs_df)


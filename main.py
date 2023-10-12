import spotipy
from spotipy.oauth2 import SpotifyOAuth
import cred
import pandas as pd

scope = 'user-read-recently-played'

sp = spotipy.Spotify(
    auth_manager=SpotifyOAuth(client_id=cred.client_id, client_secret=cred.client_secret, redirect_uri=cred.redirect_url, scope=scope)
    )

results = sp.current_user_recently_played()

song_names = []
artist_names = []
played_at_list = []
date_timestamps = []

for song in results['items']:
    song_names.append(song['track']['name'])
    artist_names.append(song['track']['artists'][0]['name'])
    played_at_list.append(song['played_at'])
    date_timestamps.append(song['played_at'][0:10])

song_dict = {
    'song_name': song_names,
    'artist_name': artist_names,
    'played_at': played_at_list,
    'date_timestamp': date_timestamps
}

df = pd.DataFrame(song_dict, columns=['song_name','artist_name','played_at','date_timestamp'])

print('HOORAY')


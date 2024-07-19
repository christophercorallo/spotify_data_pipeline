import spotipy
from spotipy.oauth2 import SpotifyOAuth
import cred
import pandas as pd
import psycopg2
import psycopg2.extras as extras
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

def connect_to_postgres():
    # connect to postgres database using psycopg
    conn = psycopg2.connect(database = "spotify_data", 
                    user = cred.postgres_username, 
                    host= 'localhost',
                    password = cred.postgres_password,
                    port = 5432,
                    options="-c search_path=spotify_user_data")
    return conn # return connection, NOT CURSOR

def remove_duplicates(con,song_df):
    # initialize cursor
    cur = con.cursor()
    # obtain most recent song in db
    cur.execute("select \"PLAYED_AT\" from SPOTIFY_USER_DATA.\"recently_played\" order by \"PLAYED_AT\" desc limit 1")
    most_recent_str = cur.fetchall()
    most_recent = datetime.datetime.strptime(most_recent_str[0][0], '%Y-%m-%dT%H:%M:%S.%fZ')
    # remove any songs obtained from spotify api after most_recent
    for id, date in enumerate(song_df['PLAYED_AT']):
        current_date = datetime.datetime.strptime(date, '%Y-%m-%dT%H:%M:%S.%fZ')
        if current_date <= most_recent:
            song_df.drop(id, inplace = True)

def write_data_to_postgres(con,song_df):
    # initialize cursor
    cur = con.cursor()
    # generate query to insert data
    tableName = 'recently_played'
    dataset = [tuple(x) for x in song_df.to_numpy()]
    cols = "\"SONG_NAME\",\"ARTIST_NAME\",\"PLAYED_AT\""
    query  = "INSERT INTO %s(%s) VALUES %%s" % (tableName, cols)
    
    # insert data
    try:
        extras.execute_values(cur,query,dataset)
        con.commit()
    finally:
        # close cursor
        cur.close()
    
recent_songs = connect_to_spotify('user-read-recently-played')
recent_songs_df = load_songs_to_df(recent_songs)
connection = connect_to_postgres()
remove_duplicates(connection,recent_songs_df)
write_data_to_postgres(connection,recent_songs_df)

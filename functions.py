import datetime
import pandas as pd
import calendar

# FUNCTIONS NO LONGER IN USE BUT COULD BE USEFUL LATER

def validate_data(df: pd.DataFrame):
    # Make sure primary key (played_at) is unique, remove songs that were not played yesterday
    
    # check if df is empty
    if df.empty:
        print('No song downloaded')
        return False

    # primary key check
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception('Primary key is not unique')
    
def get_yesterdays_songs(sp_auth, song_dict: dict):

    two_hours = datetime.datetime.now() - datetime.timedelta(hours=2)
    two_hours_unix = calendar.timegm(two_hours.utctimetuple())

    results = sp_auth.current_user_recently_played(after = two_hours_unix)

    for song in results['items']:
        song_dict['song_name'].append(song['track']['name'])
        song_dict['artist_name'].append(song['track']['artists'][0]['name'])
        song_dict['played_at'].append(song['played_at'])
        song_dict['date_timestamp'].append(song['played_at'][0:10])

    song_df = pd.DataFrame(song_dict, columns=['song_name','artist_name','played_at','date_timestamp'])

    # yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).date()
    yesterday = (datetime.datetime.now()).date()

    if yesterday == datetime.datetime.strptime(song_dict['date_timestamp'][-1], '%Y-%m-%d').date():

        # new_before = song_dict['played_at'][-1]
        new_before = datetime.datetime.strptime(song_dict['played_at'][-1], '%Y-%m-%dT%H:%M:%S.%fZ')
        # new_before_unix = time.mktime(new_before.timetuple()) * 1000
        new_before_unix = calendar.timegm(new_before.utctimetuple())
        get_yesterdays_songs(sp_auth, song_dict, before_date = new_before_unix)
    
    return song_df

def connect_to_snowflake():
    # connect to snowflake
    con = snowflake.connector.connect(
        user = cred.sf_username,
        password = cred.sf_password,
        account = cred.sf_account,
        warehouse = cred.sf_warehouse,
        database = cred.sf_database,
        schema = cred.sf_schema
        )
    return con

def write_data_to_snowflake(song_df, con):

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

def fetch_top_weekly_songs(con, close_connection = True):
  # query data using connection
  try:
    cur = con.cursor()
    cur.execute(
      ("with last_weeks_songs as ("
      "select distinct * "
      "from SPOTIFY_DATA.SPOTIFY_RECENTLY_PLAYED.SPOTIFY_RECENTLY_PLAYED "
      "WHERE \"PLAYED_AT\" >= current_date - interval '7 days' "
      "order by played_at desc"
      ") "
      "select song_name, count(song_name) as total "
      "from last_weeks_songs "
      "group by 1 "
      "order by total desc "
      "limit 5;"
      )
    )
    # create top artist and song count lists for dataframe
    top_songs, top_song_count = [], []
    for (col1, col2) in cur:
        top_songs.append(col1)
        top_song_count.append(col2)
    
    # create dataframe
    top_songs_df = pd.DataFrame({"Song": top_songs, "Song Count": top_song_count}, columns=["Song","Song Count"])
    return top_songs_df

  finally:
    if close_connection:
      # close connection
      con.close()

def fetch_top_weekly_artists(con, close_connection = True):
  # query data using connection
  try:
    cur = con.cursor()
    cur.execute(
      ("with last_weeks_artists as ("
      "select distinct * "
      "from SPOTIFY_DATA.SPOTIFY_RECENTLY_PLAYED.SPOTIFY_RECENTLY_PLAYED "
      "WHERE \"PLAYED_AT\" >= current_date - interval '7 days' "
      "order by played_at desc"
      ") "
      "select artist_name, count(artist_name) as total "
      "from last_weeks_artists "
      "group by 1 "
      "order by total desc "
      "limit 5;"
      )
    )
    # create top artist and song count lists for dataframe
    top_artists, top_artist_count = [], []
    for (col1, col2) in cur:
        top_artists.append(col1)
        top_artist_count.append(col2)
    
    # create dataframe
    top_artist_df = pd.DataFrame({"Artist": top_artists, "Artist Song Count": top_artist_count}, columns=["Artist","Artist Song Count"])
    return top_artist_df

  finally:
    if close_connection:
      # close connection
      con.close()

def load_df_to_csv(df):
    last_csv_row = pd.read_csv('spotify_listening_data.csv').iloc[-1:,:].values
    if last_csv_row.any():
        most_recent = datetime.datetime.strptime(last_csv_row[0,3], '%Y-%m-%dT%H:%M:%S.%fZ')

        for id, date in enumerate(df['PLAYED_AT']):
            current_date = datetime.datetime.strptime(date, '%Y-%m-%dT%H:%M:%S.%fZ')
            if current_date <= most_recent:
                df.drop(id, inplace = True)

    df.to_csv('spotify_listening_data.csv', mode='a', header=False)
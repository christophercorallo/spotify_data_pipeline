import datetime
import pandas as pd
import time
import calendar

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
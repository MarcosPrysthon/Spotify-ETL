import pandas as pd
import json
import requests
import os
from dotenv import load_dotenv

import sqlite3
import sqlalchemy
from sqlalchemy.orm import sessionmaker

from datetime import datetime
import datetime

def check_last_24h(df: pd.DataFrame):
    # check all songs are from the last 24 hours. if not, update dataframe

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    today_timestamp = today.strftime('%Y-%m-%d')
    yesterday_timestamp = yesterday.strftime('%Y-%m-%d')

    df.drop(df[(df['Timestamp'] != today_timestamp) & (df['Timestamp'] != yesterday_timestamp)].index, inplace=True)

def validate_data(df: pd.DataFrame):
    if df.empty:
        print("No songs downloaded. Finishing execution")
        return False
    
    # primary key check
    if not pd.Series(df['Played at']).is_unique:
        raise Exception("Primary Key Check violated - Duplicate values")

    # check for missing data
    if df.isnull().values.any():
        raise Exception("Missing values")

    check_last_24h(df)

    return True

load_dotenv()

DATABASE_LOCATION = os.getenv('DATABASE_LOCATION')
USER_ID = os.getenv('USER_ID')
TOKEN = os.getenv('TOKEN')

headers = {
    'Accept': 'application/json',
    'Content-Type': 'application/json',
    'Authorization': 'Bearer {token}'.format(token=TOKEN)
}

# yesterday date in unix format
today = datetime.datetime.now()
yesterday = today - datetime.timedelta(days=1)
yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

# requesting all played tracks after yesterday (in the last 24h)
base_url = 'https://api.spotify.com/v1/me/player/recently-played?after={time}&limit=50'.format(time=yesterday_unix_timestamp)

res = requests.get(base_url, headers=headers)
data = res.json()

# extract value info
song_name = []
artist_name = []
played_at = []
song_timestamp = []

for song in data['items']:
    song_name.append(song['track']['name'])
    artist_name.append(song['track']['album']['artists'][0]['name'])
    played_at.append(song['played_at'])
    song_timestamp.append(song['played_at'][0:10])

# creating pandas dataframe
songs_dict = {
    'Name': song_name,
    'Artist': artist_name,
    'Played at': played_at,
    'Timestamp': song_timestamp
}

song_df = pd.DataFrame(songs_dict)
print(song_df)

# Validate df
if validate_data(song_df):
    print("Data is valid. Proceed to Load stage")
    print(song_df)
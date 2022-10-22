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
base_url = 'https://api.spotify.com/v1/me/player/recently-played?after={time}'.format(time=yesterday_unix_timestamp)

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
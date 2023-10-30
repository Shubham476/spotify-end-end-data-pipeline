import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime

def lambda_handler(event, context):
    
    client_id_env = os.environ.get('client_id')
    client_secret_env = os.environ.get('client_secret')
    
    #authentication for extraction of data from spontify
    client_credentials_manager = SpotifyClientCredentials(client_id=client_id_env, client_secret=client_secret_env)
    #spontify object
    sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)
    playlist_link="https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF"
    playlist_uri = playlist_link.split('/')[-1]
    data = sp.playlist_tracks(playlist_uri)
    
    filename = "spontify_raw_" + str(datetime.now()) +".json"
    client = boto3.client('s3')
    
    client.put_object(
    Bucket='spontify-etl-project-useast1-dev',
    Key='raw-input-data/to-process-data/' + filename,
    Body=json.dumps(data)
    )
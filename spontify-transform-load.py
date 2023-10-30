import json
import boto3
import pandas as pd
from datetime import datetime
from io import StringIO

def album(data):
    album_list = []
    for row in data['items']:
        album_id = row['track']['album']['id']
        album_name = row['track']['album']['name']
        album_release_data = row['track']['album']['release_date']
        album_total_tracks = row['track']['album']['total_tracks']
        album_url = row['track']['album']['external_urls']['spotify']
        album_element = {'album_id':album_id, 'album_name':album_name, 'album_release_data':album_release_data, 'album_total_tracks':album_total_tracks,
                    'album_url':album_url}
        album_list.append(album_element)
    return album_list

def artist(data):
    artist_list = []
    for row in data['items']:
        for key,value in row.items():
            if(key=="track"):
                for artist in value['artists']:
                    artist_dict = {'artist_name':artist['name'], 'artist_id':artist['id'], 'Eexternal_url':artist['href']}
                    artist_list.append(artist_dict)
    return artist_list
    
def songs(data):
    song_list = []
    for row in data['items']:
        song_id = row['track']['id']
        song_name = row['track']['name']
        song_duration = row['track']['duration_ms']
        song_url = row['track']['external_urls']['spotify']
        song_popularity = row['track']['popularity']
        song_added = row['added_at']
        album_id = row['track']['album']['id']
        artist_id = row['track']['album']['artists'][0]['id']
        song_element = {'song_id':song_id, 'song_name':song_name, 'song_duration':song_duration, 'song_url':song_url, 'song_popularity':song_popularity,
                   'song_added':song_added, 'album_id':album_id, 'artist_id':artist_id}
        song_list.append(song_element)
    return song_list
    

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    Bucket = "spontify-etl-project-useast1-dev"
    Key = "raw-input-data/to-process-data/"
    
    spontify_data = []
    spontify_key = []
    for file in (s3.list_objects(Bucket=Bucket, Prefix=Key)['Contents']):
        file_key = file['Key']
        if file_key.split('.')[-1]=="json":
            # Get the file inside the S3 Bucket
            s3_response = s3.get_object(Bucket=Bucket , Key=file_key)
            s3_object_body = s3_response['Body']
            # Read the data in bytes format
            content = s3_object_body.read()
            # convert data in jason 
            json_dict = json.loads(content)
            spontify_data.append(json_dict)
            spontify_key.append(file_key)
            
    for data in spontify_data:
        album_list = album(data)
        artist_list = artist(data)
        songs_list = songs(data)
    
        # convert data to dataframe & remove duplicate
        album_df = pd.DataFrame.from_dict(album_list)
        album_df = album_df.drop_duplicates(subset=['album_id'])
    
        artist_df = pd.DataFrame.from_dict(artist_list)
        artist_df = artist_df.drop_duplicates(subset=['artist_id'])
    
        songs_df = pd.DataFrame.from_dict(songs_list)
    
        # convert datatypes of dates column to datetime
        album_df['album_release_data'] = pd.to_datetime(album_df['album_release_data'])
        songs_df['song_added'] = pd.to_datetime(songs_df['song_added'])
        
        artist_key = "transformed-ouput-data/artist-data/" + "artist_transformed_" + str(datetime.now()) +".csv"
        artist_file = StringIO() 
        artist_df.to_csv(artist_file, index=False)
        artist_content = artist_file.getvalue()
        s3.put_object(Bucket=Bucket, Key=artist_key , Body=artist_content)
        
        album_key = "transformed-ouput-data/album-data/" + "album_transformed_" + str(datetime.now()) +".csv"
        album_file = StringIO() 
        album_df.to_csv(album_file, index=False)
        album_content = album_file.getvalue()
        s3.put_object(Bucket=Bucket, Key=album_key , Body=album_content)
        
        # put transformed album data into s3 bucket
        songs_key = "transformed-ouput-data/songs-data/" + "songs_transformed_" + str(datetime.now()) +".csv"
        #creating string file like object
        song_file = StringIO() 
        #convert df to csv and push data to file
        songs_df.to_csv(song_file, index=False) 
        #get the data from file
        song_content = song_file.getvalue() 
        s3.put_object(Bucket=Bucket, Key=songs_key , Body=song_content) 
        
        
    
    s3_resource = boto3.resource('s3')

    for key in spontify_key:
        copy_source = {'Bucket': Bucket, 'Key': key}
        s3_resource.meta.client.copy(copy_source, Bucket, 'raw-input-data/processed-data/'+key.split("/")[-1])
        s3_resource.Object(Bucket, key).delete()
        
    
    
        


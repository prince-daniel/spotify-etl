import polars as pl
import asyncio
import os
from async_api import album as async_al
from async_api import track as async_tr
from api import artist as artist_api
from aws.client import S3

# Configuring logging
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                    level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')

BUCKET = ''
S3_KEY = '{}/{}.parquet'

# DynamoDB Table which stores artists details along with flag columns
# The table contains columns such as :
#                                     - name (S)
#                                     - id (S)
#                                     - is_extracted (BOOL)
#                                     - is_transformed (BOOL)
ARTISTS_DDB_TABLE = ''

# DynamoDB Table which stores the ETL meta-data
# The table contains column:
#                           - task (S)
#                           - is_first_run (BOOL)
META_DDB_TABLE = ''

s3_client = S3().get_client()

is_first_run = dynamo_ops.get_value(table_name=META_DDB_TABLE, key='task', value='extract')['is_first_run']['BOOL']

if is_first_run:
    logging.info('extracting artists list')
    # extract the artist list and put it as artists.parquet in bucket
    current_year_artists = artist_api.get_artist_list()
    logging.info("uploading artist's data to dynamodb")
  
    for artist in current_year_artists.iter_rows():
        id = artist[0]
        name = artist[1]
        dynamo_ops.init_artist(id, name)
  
    logging.info('done uploading meta-data of artists')
    logging.info('uploading artist list to bucket')

    # uploading `artists` parquet file to S3 raw bucket 
    s3_client.put_object(Body=current_year_artists.to_pandas().to_parquet(), Bucket=BUCKET, Key='artists.parquet')
  
    logging.info('done uploading artist list to bucket')
    logging.info('setting the `is_first_run` flag to False')
  # setting the flag 'is_first_run' to False  
  dynamo_ops.update_item(table_name=META_DDB_TABLE, key='extract', value_key='is_first_run', new_value=False,
                new_value_type='BOOL')
else:
    logging.info('extracting artist data')
    
    yet_to_extract = [item for item in dynamo_ops.scan(table_name=ARTISTS_DDB_TABLE) if item['is_extracted']['BOOL'] == False ]
    for artist in yet_to_extract:
        artist_id = artist['id']['S']
        artist_name = artist['name']['S']
      
        logging.info(f'extracting data of {artist_name}')

        total_tracks_df = pl.DataFrame({
                                  'id': '',
                                  'name': '',
                                  'popularity': '',
                                  'duration_ms': '',
                                  'explicit': '',
                                  'artist_id': '',
                                  'album_id': '',
                                  'release_date': '',
                                  'release_date_precision': '',
                                  'image': '',
                                  'href': ''
                                  })

        album_ids = asyncio.run(async_al.get_artist_albums(artist_id=artist_id, type='album'))

        if album_ids.shape[0] > 0:
            album_id_list = album_ids['id'].to_list()
            # performs asynchronous api calls to get album data
            album_df = asyncio.run(async_al.get_album(album_id_list, artist_name))

            album_pd = album_df.to_pandas()
            # uploading `albums` parquet file to S3 raw bucket
            s3_client.put_object(Body=album_pd.to_parquet(), Bucket=BUCKET, Key=S3_KEY.format(artist_name,'albums'))

            for album_id in album_id_list:
                track_ids = asyncio.run(async_al.get_album_tracks(album_id))
              # performs asynchronous api calls to get track data
                track_df = asyncio.run(async_tr.get_track(track_ids, artist_id, album_id))
                total_tracks_df.extend(track_df)

        track_pd = total_tracks_df.to_pandas()

        # setting the `is_extracted` flag to True
        dynamo_ops.update_is_extracted(table_name=ARTISTS_DDB_TABLE, key=artist_name, is_extracted=True)

        # uploading `tracks` parquet file to S3 raw bucket
        s3_client.put_object(Body=track_pd.to_parquet(), Bucket=BUCKET, Key=S3_KEY.format(artist_name,'tracks'))

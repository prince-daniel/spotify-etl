import asyncio
import aiohttp
import polars as pl
import time
import json
import pprint
from api import auth
from api import market

pp = pprint.PrettyPrinter(depth=4)

async def get_album(album_ids, artist_name):
    calls = []
    albums = []

    url = 'https://api.spotify.com'
    end_point = '/v1/albums/{}'

    headers = auth.get_bearer_token()

    params = {
        'market': market.market_codes
    }

    async with aiohttp.ClientSession(headers=headers) as session:
        for album_id in album_ids:
            calls.append(session.get(url=f'{url}{end_point.format(album_id)}', params=params))
        responses = await asyncio.gather(*calls)
        for response in responses:
            album_respr = json.loads(await response.read())
            album_dict = {}
            album_dict['id'] = album_respr['id']
            album_dict['name'] = album_respr['name']
            album_dict['artist_name'] = artist_name
            album_dict['total_tracks'] = str(album_respr['total_tracks'])
            album_dict['label'] = album_respr['label']
            album_dict['popularity'] = str(album_respr['popularity'])
            album_dict['href'] = album_respr['external_urls']['spotify']

            if len(album_respr['images']) > 0:
                album_dict['image'] = album_respr['images'][0]['url']
            else:
                album_dict['image'] = ''
            albums.append(album_dict)

    return pl.DataFrame(albums)

async def get_artist_albums(artist_id, type):
    endpoint = 'https://api.spotify.com/v1/artists/{}/albums'.format(artist_id)

    headers = auth.get_bearer_token()

    params = {
        'market': market.market_codes,
        'limit': 50,
        'include_groups': type
    }

    artist_albums = []

    async with aiohttp.ClientSession(headers=headers) as session:
        while endpoint:
            response = await session.get(endpoint, params=params)
            if response.status == 200:
                response = json.loads(await response.read())
                if 'items' in response:
                    for item in response['items']:
                        album = {
                            'id': item['id'],
                            'name': item['name']
                        }
                        artist_albums.append(album)
                    endpoint = response['next']
            elif response.status == 429:
                print('Too many requests')
    return pl.DataFrame(artist_albums)

async def get_album_tracks(album_id):
    endpoint = f'https://api.spotify.com/v1/albums/{album_id}/tracks'

    headers = auth.get_bearer_token()

    params = {
        'market': market.market_codes,
        'limit': 50
    }

    album_tracks = []

    async with aiohttp.ClientSession(headers=headers) as session:
        while endpoint:
            response = await session.get(endpoint, params=params)
            response = json.loads(await response.read())
            if 'items' in response:
                for item in response['items']:
                    album_tracks.append(item['id'])
                endpoint = response['next']
    return album_tracks


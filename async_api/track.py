import asyncio
import aiohttp
import json
import pprint
import polars as pl
from api import auth
from api import market

pp = pprint.PrettyPrinter(depth=4)

async def get_track(track_ids, artist_id, album_id):
    calls = []
    tracks = []

    url = 'https://api.spotify.com'
    end_point = '/v1/tracks/{}'

    headers = auth.get_bearer_token()

    params = {
        'market': market.market_codes
    }

    async with aiohttp.ClientSession(headers=headers) as session:
        for track_id in track_ids:
            calls.append(session.get(url=f'{url}{end_point.format(track_id)}', params=params))
        responses = await asyncio.gather(*calls)
        for response in responses:
            track_response = json.loads(await response.read())
            track_dict = {
                'id': track_response['id'],
                'name': track_response['name'],
                'popularity': str(track_response['popularity']),
                'duration_ms': str(track_response['duration_ms']),
                'explicit': str(track_response['explicit']),
                'href': track_response['external_urls']['spotify'],
                'release_date': track_response['album']['release_date'],
                'release_date_precision': track_response['album']['release_date_precision'],
                'artist_id': artist_id,
                'album_id': album_id
            }
            if len(track_response['album']['images']) > 0:
                track_dict['image'] = track_response['album']['images'][0]['url']
            else:
                track_dict['image'] = ''
            tracks.append(track_dict)

    return pl.DataFrame(tracks)
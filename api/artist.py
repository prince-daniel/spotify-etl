import polars as pl
from datetime import datetime
from . import market
from .search import search

SEARCH_TYPE = 'artist'
INCLUDE_GROUPS = 'appears_on'

def get_artist_list(year=datetime.now().year, market=market.market_codes):
    artist_list = []
    OFFSET = 0
    INCR_OFFSET = 50
    LIMIT = 50

    while True:
        response = search(q=f'year:{year}', market=market, search_type=SEARCH_TYPE, offset=OFFSET,limit=LIMIT)
        artists = response['artists']['items']

        for artist in artists:
            artist_list.append({
                'id': artist['id'],
                'name': artist['name'],
                'type': artist['type'],
                'popularity': artist['popularity'],
                'genres': artist['genres'],
                'followers': artist['followers']['total'],
                'href': artist['href'],
                'image_640': artist['images'][0]['url'],
                'image_320': artist['images'][1]['url'],
                'image_160': artist['images'][2]['url']
            })

        OFFSET += INCR_OFFSET

        if response['artists']['next'] == None:
            break

    artists_df = pl.DataFrame(artist_list)
    return artists_df

from .auth import *
from . import market
from requests import get

def search(q, search_type, market = market.market_codes, offset=0, limit=50):
    SEARCH_URL = 'https://api.spotify.com/v1/search'

    headers = get_bearer_token()

    payload = {
        'q': q,
        'type': search_type,
        'offset': offset,
        'market': market,
        'limit': limit
    }

    response = get(url=SEARCH_URL, headers=headers, params=payload)
    return json.loads(response.content)
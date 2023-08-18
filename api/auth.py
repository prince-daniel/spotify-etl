import boto3
import json
from requests import post

# Secret name of Spotify Credentials stored in AWS Secrets Manager
SECRET_NAME = ''
AWS_REGION = ''

def get_spotify_secrets(secret_name, region_name):
    secrets_client = boto3.client('secretsmanager', region_name=region_name)
    spotify_secrets = json.loads(secrets_client.get_secret_value(SecretId=SECRET_NAME)['SecretString'])
    CLIENT_ID = spotify_secrets['SPOTIFY_CLIENT_ID']
    CLIENT_SECRET = spotify_secrets['SPOTIFY_CLIENT_SECRET']
    return CLIENT_ID, CLIENT_SECRET

def get_access_token():
    CLIENT_ID, CLIENT_SECRET = get_spotify_secrets(SECRET_NAME, AWS_REGION)
    SPOTIFY_ACCESS_TOKEN_URL = 'https://accounts.spotify.com/api/token'

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    data = {
        'grant_type': 'client_credentials',
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET
    }

    response = post(url=SPOTIFY_ACCESS_TOKEN_URL, headers=headers, data=data)
    return json.loads(response.content)


def get_bearer_token():
    access_token = get_access_token()
    return {
        'Authorization': f"{access_token['token_type']} {access_token['access_token']}"
    }

## Connect to Polygon.io and receive stock information to push to kafka topics

# Import libraries 
from genericpath import exists
import json
import os
from sqlite3 import connect
import requests

from dotenv import load_dotenv


# Get environment from file
load_dotenv()
POLY_BEARER_AUTH = os.getenv('POLYGONBEARERTOKEN')

# Define API url
def generate_url(ticker, time_range, from_date, to_date):
    search_url = 'https://api.polygon.io/v2/aggs/ticker/{}/range/{}/day/{}/{}?adjusted=true&sort=asc&limit=5000'.format(ticker, time_range, from_date, to_date)
    return search_url

# Define authentication
def bearer_oauth(r):
    r.headers['Authorization'] = f'Bearer {POLY_BEARER_AUTH}'
    return r

# Connect to Stcok API
def endpoint_connect(url, params):
    response = requests.get(url, params=params, auth=bearer_oauth)
    print('Polygon: ' + f"{response.status_code}")
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

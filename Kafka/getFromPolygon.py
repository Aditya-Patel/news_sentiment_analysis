## Connect to Polygon.io and receive stock information to push to kafka topics

# Import libraries 
import os
import requests
from dotenv import load_dotenv


# Get environment from file
load_dotenv()
POLY_BEARER_AUTH = os.getenv('POLYGONBEARERTOKEN')
POLY_PRICE_EP = os.getenv('POLYGONPRICEENDPOINT')
POLY_TICK_EP = os.getenv('POLYGONTICKERENDPOINT')

# Define API url
def generate_url(ticker, **params):
    if params.get('range') and params.get('from_date') and params.get('to_date'):
        search_url = POLY_PRICE_EP + '{}/range/{}/{}/{}/{}?adjusted=true&sort=asc&limit=5000'.format(ticker, params.get('range'), params.get('time_span'), params.get('from_date'), params.get('to_date'))
    else:
        search_url = POLY_TICK_EP + f"{ticker}"
    return search_url

# Define authentication
def bearer_oauth(r):
    r.headers['Authorization'] = f'Bearer {POLY_BEARER_AUTH}'
    return r

# Connect to API
def endpoint_connect(url):
    response = requests.get(url, auth=bearer_oauth)
    print('Polygon: ' + f"{response.status_code}")
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()


def get_json_response(ticker, **params):
    # Generate URL
    if params.get('range') and params.get('from_date') and params.get('to_date'):
        url = generate_url(ticker, range=params.get("range"), time_span=params.get("time_span"), from_date=params.get("from_date"), to_date=params.get("to_date"))
    else:
        url = generate_url(ticker)

    # Connect to API
    response = endpoint_connect(url)

    # Return response
    return response
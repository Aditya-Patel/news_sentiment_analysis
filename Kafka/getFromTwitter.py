## Connect to twitter and receive twitter information to push to kafka topics

# Import libraries 
from genericpath import exists
import json
import os
from sqlite3 import connect
import requests

from dotenv import load_dotenv

# Load environment variables
load_dotenv()
BEARER_TOKEN = os.getenv('BEARERTOKEN')
APIKEY = os.getenv('APIKEY')
APISECRETKEY = os.getenv('APIKEYSECRET')
CONSUMERKEY = os.getenv('CLIENTKEY')    
CONSUMERSECRETKEY = os.getenv('CLIENTSECRETKEY')

# API endpoint URL
search_url = "https://api.twitter.com/2/tweets/search/recent?max_results=100"

good_sentiments = {
    'bullish':1,
    'excite':1,
    'good ':1,
    'great':2,
    'amazing':3,
    'raised':1,
    'increase':1,
    'profit':1,
    'best':3,
    'exceed':1,
    'strong earnings':1,
    'upgrade':2,
    'way to go':2,
    ' up ':1,
    'strong':1,
    'stronger':2,
    'strongest':3,
    '!':1,
    'aggressive':1
}

bad_sentiments = {
    'bearish':1,
    'disappointed':2,
    'bad':1,
    'worse':2,
    'worst':3,
    'poor':1,
    'decrease':1,
    'lower':1,
    'concerned':2,
    'loss':1,
    'not':1,
    'downgrade':2,
    'need to improve':2,
    ' down ':1
}

def bearer_oauth(r):
    r.headers["Authorization"] = f"Bearer {BEARER_TOKEN}"
    r.headers["User-Agent"] = "v2RecentSearchPython"
    return r

def connect_to_endpoint(url, params):
    response = requests.get(url, params=params, auth=bearer_oauth)
    print('Twitter: ' + f"{response.status_code}")
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

def calculate_sentiment_score(text):
    score = pos_ind_score = neg_ind_score = 0
    for indicator, value in good_sentiments.items():
        if indicator in text:
            score += value * text.count(indicator)
        pos_ind_score += score
        score = 0

    for indicator, value in bad_sentiments.items():
        if indicator in text:
            score += value * text.count(indicator)
        neg_ind_score -= score
        score = 0

    return (pos_ind_score + neg_ind_score)

def create_string(score, ticker, tweet_text=""):
    twt_predicate = ""
    if not tweet_text:
        print(f"No tweets returned for ticker symbol {ticker}")
        twt_predicate = f"No tweets found for {ticker}"
    else:
        twt_predicate = tweet_text

    return f"{twt_predicate} | TWEET SENTIMENT SCORE: {score}"
    
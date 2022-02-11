## Connect to twitter and receive twitter information to push to kafka topics

# Import libraries 
import os
import requests
import sentimentAnalysis as san

from dotenv import load_dotenv

# Load environment variables
load_dotenv()
API_KEY = os.getenv('NEWSAPIKEY')
search_url = os.getenv('NEWSAPIENDPOINT')

def bearer_oauth(r):
    r.headers['X-Api-Key'] = API_KEY
    return r

def connect_to_endpoint(url, params):
    response = requests.get(url, params=params, auth=bearer_oauth)
    print('NewsAPI: ' + f"{response.status_code}")
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

def print_article_details(article):
    print(f"Title: {article['title']} \nDescription: {article['description']}\nSource: {article['url']}")
    sent_bin, sent_text = san.get_text_sentiment(f"{article['title'].lower()} {article['description'].lower()} {article['content'].lower()}")
    print(f"Article Sentiment: {sent_text}\n")
    return sent_bin
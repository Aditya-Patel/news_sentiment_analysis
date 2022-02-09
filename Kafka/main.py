from re import A
from tracemalloc import start
from types import NoneType
from venv import create
import getFromTwitter as twt
import getFromPolygon as poly
import kafkaToolbox as kfk
from datetime import datetime, date, timedelta
import datetime

twt_string = ""

def set_dates(range):
    present = date.today().isoformat()
    past = (date.today() - timedelta(days=range)).isoformat()
    
    iso_to_day = f"{present}T00:00:00Z"
    iso_from_day = f"{past}T00:00:00Z"

    return iso_from_day, iso_to_day, past, present
    

def main():
    # input_consumer = kfk.createKafkaConsumer('localhost:9092', 'user-input')
    # output_producer = kfk.createKafkaProducer('localhost:9092', 'api-output')

    ticker = input('Enter stock symbol: ').upper()
    range = 1
    iso_from_date, iso_to_date, from_date, to_date = set_dates(range)

    twt_query_params =  {'query': f'#{ticker} -has:mentions -is:reply -is:quote -is:reply is:verified lang:en', 'start_time':iso_from_date,'end_time':iso_to_date}
    twt_json_response = twt.connect_to_endpoint(twt.search_url, twt_query_params)
    
    poly_search_url = poly.generate_url(ticker, range, from_date, to_date)
    poly_json_response = poly.endpoint_connect(poly_search_url, {'query':''})

    # Parse Twitter API Data
    if 'data' in twt_json_response.keys():
        for tweet in twt_json_response['data']:
            tweet_score = twt.calculate_sentiment_score(tweet['text'])
            twt_string = twt.create_string(tweet_score, ticker, tweet['text'])
            print(f"{twt_string}\n")
            # kfk.send(topic='api-output', key=ticker, value=twt_string.encode('utf-8'), producer=output_producer)
    else:
        tweet_score = 0
        twt_string = twt.create_string(tweet_score, ticker)
        print(twt_string)
        # kfk.send(topic='api-output', key=ticker, value=twt_string.encode('utf-8'), producer=output_producer)

    # Parse Polygon API Response
    if 'results' in poly_json_response.keys():
        for result in poly_json_response['results']:
            poly_string = f"{ticker}: Open: {result['o']} | Close: {result['c']} | High: {result['h']} | Low: {result['l']}"
            print(poly_string)
            # kfk.send(topic='api-output', key=ticker, value=poly_string.encode('utf-8'), producer=output_producer)

if __name__ == '__main__':
    main()
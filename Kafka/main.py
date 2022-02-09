import getFromTwitter as twt
import getFromPolygon as poly
import kafkaToolbox as kfk
from datetime import date, timedelta

def main():
    ticker = input('Enter stock symbol: ').upper()
    range = 1
    today = date.today()
    yesterday = date.today() - timedelta(days=1)

    
    twt_query_params =  {'query': '#' + ticker + ' -has:mentions -is:reply -is:quote -is:reply is:verified lang:en'}
    twt_json_response = twt.connect_to_endpoint(twt.search_url, twt_query_params)
    
    poly_search_url = poly.generate_url(ticker, range, yesterday, today)
    poly_json_response = poly.endpoint_connect(poly_search_url)

    kafka_producer = kfk.createKafkaProducer
    
    if 'data' in twt_json_response.keys():
        for tweet in twt_json_response['data']:
            tweet_score = twt.calculate_sentiment_score(tweet['text'])
            # print(tweet['text'])
            # print('TWEET SENTIMENT SCORE: {}'.format(tweet_score) + '\n')
            # ticker_score += tweet_score
            # kfk.send(ticker, tweet_score, tweet['text'], kafka_producer)            

    

if __name__ == '__main__':
    main()
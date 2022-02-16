from http import server
from re import A
from types import NoneType
from venv import create
import sentimentAnalysis as san
import getFromNews as news
import getFromPolygon as poly
import kafkaToolbox as kfk
from datetime import date, timedelta

polygon_topic = 'polygon-api-output'
news_topic = 'news-api-output'

def news_api_call(ticker, to_date, date_rnge):
    from_date = date.today() - timedelta(days=date_rnge)
    ticker_name = (poly.get_json_response(ticker))['results']['name'].split(' ')[0].split(',')[0]
    news_query_params =  {'language':'en', 'q':f'/"{ticker_name}/", {ticker}', 'searchIn':'title,description', 'sortBy':'popularity', 'from':f'{to_date}'}
    news_json_response = news.connect_to_endpoint(news.search_url, news_query_params)
    
    article_dict = {}
    for value in range(-4, 5):
        article_dict[value] = list()

    for article in news_json_response['articles']:
        if ticker_name in article['title'] or ticker_name in article['description']:
            key = news.print_article_details(article)
            value = article['title']
            article_dict[key].append(value)

    news_json_out = {
        'ticker': ticker,
        'article-scores':article_dict
    }

    return news_json_out            


def main():
    # Initialize Kafka producers and consumers
    # input_consumer = kfk.createKafkaConsumer('localhost:9092', 'user-input')
    news_output_producer = kfk.createKafkaProducer(server='localhost:9092')
    polygon_output_producer = kfk.createKafkaProducer(server='localhost:9092')

    ticker = input('Enter stock symbol: ').upper()
    time_gran = input('Enter time granularity: ')
    time_unit = input('Enter time units: ')
    
    date_rnge = 1
    
    to_date = date.today()

    
    polygon_dict = {}
    
            
    poly_search_url = poly.generate_url(ticker, range=time_gran, time_span=time_unit, from_date=from_date, to_date=to_date)
    poly_json_response = poly.endpoint_connect(poly_search_url)

    if 'results' in poly_json_response.keys():
        results = poly_json_response['results']
        for result in results:
            for key, value in result.items():
                if key in polygon_dict.keys():
                    polygon_dict[key].append(value)
                else:
                    polygon_dict[key] = list()
                    polygon_dict[key].append(value)


    

    polygon_json_out = {
        'ticker': ticker,
        'ticker-prices':polygon_dict
    }

    news_output_producer.send(topic=news_topic, key=ticker, value=news_api_call(ticker, to_date, date_rnge))
    polygon_output_producer.send(topic=polygon_topic, key=ticker, value=polygon_api_call(ticker, to_date, date_rnge))

if __name__ == '__main__':
    main()
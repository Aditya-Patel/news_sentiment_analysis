from re import A
from types import NoneType
from venv import create
import sentimentAnalysis as san
import getFromNews as news
import getFromPolygon as poly
import kafkaToolbox as kfk
from datetime import date, timedelta
    
def main():
    # input_consumer = kfk.createKafkaConsumer('localhost:9092', 'user-input')
    # output_producer = kfk.createKafkaProducer('localhost:9092', 'api-output')

    ticker = input('Enter stock symbol: ').upper()
    ticker_name = (poly.get_json_response(ticker))['results']['name'].split(' ')[0].split(',')[0]
    rnge = 1
    from_date = date.today() - timedelta(days=rnge)
    to_date = date.today()

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
            
    poly_search_url = poly.generate_url(ticker, range=range, from_date=from_date, to_date=to_date)
    poly_json_response = poly.endpoint_connect(poly_search_url)

    json_output = {
        'id': ticker,
        
    }

if __name__ == '__main__':
    main()
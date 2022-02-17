import json
import sentimentAnalysis as san
import getFromNews as news
import getFromPolygon as poly
import kafkaToolbox as kfk
from datetime import date, timedelta, datetime

# Define constants
polygon_topic = 'polygon-api-output'
news_topic = 'news-api-output'
time_gran = 3
time_unit = 'hour'
date_rnge = 1    
cur_date = date.today()

# News API Call: performs call to news API based on ticker symbol and returns the JSON response from the articles scored
def news_api_call(ticker, cur_date):
    from_date = cur_date - timedelta(days=2)
    to_date = cur_date - timedelta(days=1)
    ticker_name = (poly.get_json_response(ticker))['results']['name'].split(' ')[0].split(',')[0]

    news_query_params =  {'language':'en', 'q':f'/"{ticker_name}/", {ticker}', 'searchIn':'title,description', 'sortBy':'popularity', 'from':f'{from_date}T23:59:59Z', 'to':f'{to_date}T00:00:00Z'}
    news_json_response = news.connect_to_endpoint(news.search_url, news_query_params)
    news_json_output = {
        'ticker':ticker,
        'articles':[]
    }
    
    for article in news_json_response['articles']:
        article_dict = {}
        if ticker_name in article['title'] or ticker_name in article['description']:
            article_dict['title'] = article['title']
            article_dict['publish_time'] = article['publishedAt']
            article_dict['sentiment_score'] = news.print_article_details(article)
        news_json_output['articles'].append(article_dict)

    return news_json_output

# Performs a call to the Polygon.io API to retrieve the daily valuation of prices
def polygon_api_call(ticker, to_date, date_rnge, time_gran, time_unit):
    from_date = to_date - timedelta(days=date_rnge)
        
    poly_search_url = poly.generate_url(ticker, range=time_gran, time_span=time_unit, from_date=from_date, to_date=to_date)
    poly_json_response = poly.endpoint_connect(poly_search_url)

    polygon_json_output = {
        'ticker':ticker,
        'prices':[]
    }

    if 'results' in poly_json_response.keys():
        results = poly_json_response['results']
        for result in results:
            result_dict = {}
            for key, value in result.items():
                if key == 't':
                    result_dict[key] = (datetime.fromtimestamp(value/1000.0).strftime("%m-%d-%y %H:%M:%S"))
                else:
                    result_dict[key] = value
            polygon_json_output['prices'].append(result_dict)

    return polygon_json_output

def print_last_item(enumerator, length, value, nestingCount):
    if enumerator != length:
        print_json(value, nestingCount=nestingCount, lastItem=False)
    else:
        print_json(value, nestingCount=nestingCount, lastItem=True)

def print_json(json_dict, nestingCount=0, lastItem = True):
    newNestingCount = nestingCount + 1
    if nestingCount == 0:
        print('{')
    else:
        print(newNestingCount*'   '+'{')

    for enum, (key, value) in enumerate(json_dict.items()):
        print(2*newNestingCount*'   ' + f'{key}:', end=' ')
        if type(value) == dict:
            print_last_item(enum, (len(json_dict.items())-1), value, newNestingCount)
        elif type(value) == list:
            print('[')
            for list_enum, item in enumerate(value):
                print_last_item(list_enum, (len(value)-1), item, newNestingCount)
            if enum != (len(json_dict.items())-1):
                print(newNestingCount*'   '+'],')    
            else:
                print(newNestingCount*'   '+']')
        else:
            if enum != (len(json_dict.items())-1):
                print(value, end=',\n')
            else:
                print(value)
    print(nestingCount*'   ', end='')
    if not lastItem:
        print(nestingCount*'   '+'},')
    else:
        print(nestingCount*'   '+'}')
            
def main():
    # Initialize Kafka producers and consumers
    input_consumer = kfk.createKafkaConsumer(bootstrap_server='localhost:9092', topics='user-input')
    news_output_producer = kfk.createKafkaProducer(server='localhost:9092')
    polygon_output_producer = kfk.createKafkaProducer(server='localhost:9092')

    for value in input_consumer.poll(timeout_ms=5000, max_records=1).values():
        ticker = value[0].value.decode('utf-8')
        news_json_out = news_api_call(ticker, cur_date)
        polygon_json_out = polygon_api_call(ticker, cur_date, date_rnge, time_gran, time_unit)
        print_json(news_json_out)
        print_json(polygon_json_out)
        news_output_producer.send(topic=news_topic, key=ticker.encode('utf-8'), value=json.dumps(news_json_out).encode('utf-8'))
        polygon_output_producer.send(topic=polygon_topic, key=ticker.encode('utf-8'), value=json.dumps(polygon_json_out).encode('utf-8'))
    
if __name__ == '__main__':
    while True:
        main()
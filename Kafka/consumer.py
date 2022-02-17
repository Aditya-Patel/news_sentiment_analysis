import json
from kafkaToolbox import createKafkaConsumer

polygon_topic = 'polygon-api-output'
news_topic = 'news-api-output'

def consume_kafka():
    # Consume and dump JSON files from kafka 
    kafka_consumer = createKafkaConsumer(topics=[news_topic, polygon_topic])
    
    for key, value in kafka_consumer.poll(timeout_ms=5000, max_records=2).items():
        if key.TopicPartition.topic == polygon_topic:
            poly_write = {value[0].key.decode('utf-8'): value[0].value.decode('utf-8')}
            with open('../json_dumps/poly_json.json','w') as json_file:
                json.dump(poly_write, json_file)
        elif key.TopicPartition.topic == news_topic:
            news_write = {value[0].key.decode('utf-8'): value[0].value.decode('utf-8')}
            with open('../json_dumps/poly_json.json','w') as json_file:
                json.dump(news_write, json_file)

if __name__ == '__main__':
    consume_kafka()
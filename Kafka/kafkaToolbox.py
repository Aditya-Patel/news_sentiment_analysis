# Python script to hold Kafka related python functions
from kafka import KafkaConsumer, KafkaProducer

# Set up Kafka Producer
def createKafkaProducer(server, topics):
    return KafkaProducer(bootstrap_servers = server, topics=topics)

def send(topic, key, value, producer):
    producer.send(topic, value=bytes(value, 'utf-8'), key=key)

def createKafkaConsumer(server, topics):
    return KafkaConsumer(bootstrap_servers = server, topics=topics)
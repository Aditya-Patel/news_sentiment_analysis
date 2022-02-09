# Python script to hold Kafka related python functions
from kafka import KafkaConsumer, KafkaProducer

# Set up Kafka Producer
def createKafkaProducer():
    return KafkaProducer(bootstrap_servers = 'localhost:9092')
    

def send(topic, key, value, producer):
    producer.send(topic, value=bytes(value, 'utf-8'), key=key)

def createKafkaConsumer():
    return KafkaConsumer(bootstrap_servers = 'localhost:9092')
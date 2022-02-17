# Python script to hold Kafka related python functions
from kafka import KafkaConsumer, KafkaProducer

# Set up Kafka Producer
def createKafkaProducer(server):
    return KafkaProducer(bootstrap_servers = server)

def send(topic, key, value, producer):
    producer.send(topic, value=bytes(value, 'utf-8'), key=key)

def createKafkaConsumer(bootstrap_server='localhost:9092', topics='user-input'):
    consumer = KafkaConsumer(bootstrap_servers=bootstrap_server)
    consumer.subscribe(topics=topics)
    return consumer
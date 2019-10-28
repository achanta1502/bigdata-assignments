from kafka import KafkaConsumer
import json, sys


def connect_kafka_consumer(topic):
    consumer = KafkaConsumer(topic,
                             bootstrap_servers=['localhost:9092'],
                             auto_offset_reset='earliest'
                             )
    for msg in consumer:
        print(msg.value)
        print('\n')

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("not enough arguments")
        exit()
    topic = sys.argv[1]
    connect_kafka_consumer(topic)
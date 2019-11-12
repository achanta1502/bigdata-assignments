from kafka import KafkaConsumer
import csv
import sys


def data_from_kafka_consumer(topic):
    consumer = KafkaConsumer(topic,
                             bootstrap_servers=['localhost:9092'],
                             auto_offset_reset='earliest'
                             )
    with open('/home/achanta/Desktop/output.csv', mode='a') as test_file:
        writer = csv.writer(test_file)
        for msg in consumer:
            decoded = msg.value.decode('utf-8')
            cols  = decoded.split("||")
            asciidata = cols[1].encode("ascii", "ignore")
            writer.writerow([asciidata, cols[0]])
            print(asciidata)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("not enough arguments")
        exit()
    topic = sys.argv[1]
    data_from_kafka_consumer(topic)



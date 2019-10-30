from kafka import KafkaConsumer
import json, sys
import os, re
import pandas as pd
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem.porter import PorterStemmer

def data_from_kafka_consumer(topic, time):
    data = []
    file = open("/tmp/testdata.txt", "a")
    consumer = KafkaConsumer(topic,
                             bootstrap_servers=['localhost:9092'],
                             auto_offset_reset='earliest'
                             )
    for msg in consumer:
        if time <= 0:
            break
        print(msg.value)
        a = re.sub('[^A-Za-z0-9\s]+', '', msg.value)
        print(a)
        data.append(msg.value)
        file.write(msg.value + '\n')
        time = time - 1
    #df = pd.DataFrame(data)
    file.close()

    return data

def stopwords_removal():
    return set(stopwords.words('english'))

def tokenize(sentences):
    stop_words = stopwords_removal()
    word_tokens = []
    for sentence in sentences:
        word_tokens.extend(word_tokenize(sentence))
    remove_stopwords = set(w for w in word_tokens if not w in stop_words)
    return sorted(list(remove_stopwords))

def porter(word):
    return PorterStemmer().stem(word)

def stemmer(tokenized_words):
    for i in range(len(tokenized_words)):
        tokenized_words[i] = porter(tokenized_words[i])
    return tokenized_words

def text_processing(sentences):
    words = tokenize(sentences)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("not enough arguments")
        exit()
    topic = sys.argv[1]
    time = int(sys.argv[2])
    sentence = data_from_kafka_consumer(topic, time)

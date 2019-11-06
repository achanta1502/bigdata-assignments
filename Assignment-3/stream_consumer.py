from kafka import KafkaConsumer
import json, sys
import os, re
import pandas as pd
import nltk
import numpy as np
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem.porter import PorterStemmer
import csv
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn import model_selection
from sklearn.naive_bayes import MultinomialNB
from sklearn.metrics import accuracy_score
import socket
import spark
TCP_IP = 'localhost'
TCP_PORT = 9001

# create sockets
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# s.connect((TCP_IP, TCP_PORT))
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
conn, addr = s.accept()

def data_from_kafka_consumer(topic):
    data = []
    df = pd.DataFrame()
    consumer = KafkaConsumer(topic,
                             bootstrap_servers=['localhost:9092'],
                             auto_offset_reset='earliest'
                             )
    time = 0
    start()
    for msg in consumer:
        if time > 300:
            break
        # decoded = msg.value.decode('utf-8')
        conn.send(msg.value)
        # cols  = decoded.split("||")
        # # dec = re.sub('[^A-Za-z0-9\s]+', ' ', cols[1])
        # asciidata = cols[1].encode("ascii", "ignore")
        # data.append([asciidata, cols[0]])
        # time = time + 1
        # vals = data.get(cols[0], [])
        # vals.append(re.sub('[^A-Za-z0-9\s]+', ' ', cols[-1]))
        # data[cols[0]] = vals
    # print(word_tokenize(data[0][0]))
    # df.columns = ['headline', 'label']
    #df = pd.DataFrame(data)
    # with open('/home/achanta/Desktop/output.csv', mode='a') as test_file:
    #     writer = csv.writer(test_file)
    #     for line in data:
    #         writer.writerow(line)
    # return df

def data_from_text():
    df = pd.read_csv("/home/achanta/Desktop/output.csv", delimiter=',', header=None)
    df.columns = ['review', 'label']
    return df

def stopwords_removal():
    return set(stopwords.words('english'))

def tokenize(text):
    tokens = set(word_tokenize(text))
    stop_words = stopwords_removal()
    tokens = [w for w in tokens if not w in stop_words]
    stems = [porter(item) for item in tokens]
    return tokens

def porter(word):
    return PorterStemmer().stem(word)


def vector_fit_transform(X_train, X_test):
    vect = TfidfVectorizer(tokenizer=tokenize, use_idf=True)
    train_vectors = vect.fit_transform(X_train)
    test_vectors = vect.transform(X_test)
    return (train_vectors, test_vectors)

def text_processing():
    df = data_from_text()
    return df

def train_test(df):
    X_train = df.loc[:250, 'review'].values
    Y_train = df.loc[:250, 'label'].values
    X_test = df.loc[:250, 'review'].values
    Y_test = df.loc[:250, 'label'].values
    return X_train, Y_train, X_test, Y_test

def model(X_train, y_train):
    clf = MultinomialNB().fit(X_train, y_train)
    return clf

def predict(model, test):
    return model.predict(test)

def model_building():
    df = text_processing()
    X_train, Y_train, X_test, Y_test = train_test(df)
    train_vectors, test_vectors = vector_fit_transform(X_train, X_test)
    clf = model(train_vectors, Y_train)
    predicted = predict(clf, test_vectors)
    print(accuracy_score(predicted, Y_test))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("not enough arguments")
        exit()
    topic = sys.argv[1]
    # time = int(sys.argv[2])
    mapped_data = data_from_kafka_consumer(topic)
    # print(df)
    # X_train, X_validation, y_train, y_validation = train_test(mapped_data)
    # model = model(X_train, y_train)
   #     predicted_value = predict(model, X_validation)
    # for i in range(len(predicted_value)):
    #     print(y_validation[i], predicted_value[i])
    # model_building()


from kafka import KafkaConsumer
import pandas as pd
import csv
import sys
from utils import train_test, vector_fit_transform, train_model, predict, accuracy


def data_from_kafka_consumer(topic):
    # df = pd.DataFrame()
    consumer = KafkaConsumer(topic,
                             bootstrap_servers=['localhost:9092'],
                             auto_offset_reset='earliest'
                             )
    with open('/home/achanta/Desktop/output.csv', mode='a') as test_file:
        writer = csv.writer(test_file)
        for msg in consumer:
            decoded = msg.value.decode('utf-8')
            cols  = decoded.split("||")
            # dec = re.sub('[^A-Za-z0-9\s]+', ' ', cols[1])
            asciidata = cols[1].encode("ascii", "ignore")
            writer.writerow([asciidata, cols[0]])
            print(asciidata)
        # time = time + 1
        # vals = data.get(cols[0], [])
        # vals.append(re.sub('[^A-Za-z0-9\s]+', ' ', cols[-1]))
        # data[cols[0]] = vals
    # print(word_tokenize(data[0][0]))
    # df.columns = ['headline', 'label']
    # df = pd.DataFrame(data)


    # return df


def data_from_text():
    df = pd.read_csv("/home/achanta/Desktop/output.csv", delimiter=',', header=None)
    df.columns = ['review', 'label']
    return df


def text_processing():
    df = data_from_text()
    return df


def model_building():
    df = text_processing()
    x_train, y_train, x_test, y_test = train_test(df, 0.1)
    train_vectors, test_vectors = vector_fit_transform(x_train, x_test)
    train_model(train_vectors, y_train)
    predicted = predict(test_vectors)
    print(accuracy(predicted, y_test))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("not enough arguments")
        exit()
    topic = sys.argv[1]
    # time = int(sys.argv[2])
    data_from_kafka_consumer(topic)
    # print(df)
    # X_train, X_validation, y_train, y_validation = train_test(mapped_data)
    # model = model(X_train, y_train)
    #     predicted_value = predict(model, X_validation)
    # for i in range(len(predicted_value)):
    #     print(y_validation[i], predicted_value[i])
    # model_building()


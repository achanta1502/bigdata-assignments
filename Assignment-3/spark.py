from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

def start():
    print("entered")
    TCP_IP = 'localhost'
    TCP_PORT = 9001

    conf = SparkConf()
    conf.setAppName('newsApp')
    conf.setMaster('local[2]')

    conf.set("spark.network.timeout","4200s")
    conf.set("spark.executor.heartbeatInterval","4000s")

    # create spark context with the above configuration
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")

    # create the Streaming Context from spark context with interval size 2 seconds
    ssc = StreamingContext(sc, 2)
    kafkaStream = KafkaUtils.createDirectStream(ssc, ['guardian2'], {"metadata.broker.list": "localhost:9092"})
    kafkaStream.pprint()

    ssc.start()
    ssc.awaitTermination()

if __name__ == "__main__":
    start()
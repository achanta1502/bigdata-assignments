from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.ml import Pipeline
from pyspark.sql import Row, DataFrame, SQLContext


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
    sqlContext = SQLContext(sc)
    # create the Streaming Context from spark context with interval size 2 seconds
    ssc = StreamingContext(sc, 2)
    kafkaStream = KafkaUtils.createDirectStream(ssc, ['guardian2'], {"metadata.broker.list": "localhost:9092"})
    values = kafkaStream.map(lambda x: x[1].encode("ascii", "ignore"))
    lines = values.map(lambda x: x.split("||"))
    lines.foreachRDD(process)
    # pipeline = Pipeline(stages=[])
    ssc.start()
    ssc.awaitTermination()


def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']


def process(time, rdd):
    print("========%s===========" % str(time))
    try:
        sqlContext = getSqlContextInstance(rdd.context)
        rowRdd = rdd.map(lambda w: Row(label=w[0], review=w[1]))
        # rowRdd.pprint()
        wordsDataFrame = sqlContext.createDataFrame(rowRdd)
        wordsDataFrame.show()
    except:
        print("exception")
        pass

if __name__ == "__main__":
    start()
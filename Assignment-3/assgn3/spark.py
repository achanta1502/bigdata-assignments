from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row, DataFrame, SQLContext
from pyspark.sql import SparkSession
from utils import pipeline
from stream_consumer import model_building
from elasticsearch import Elasticsearch


def elasticSearch():
    return Elasticsearch([{'host': 'localhost', 'port': 9200}])


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
    spark = SparkSession(sc)
    # create the Streaming Context from spark context with interval size 2 seconds
    ssc = StreamingContext(sc, 2)
    kafkaStream = KafkaUtils.createDirectStream(ssc, ['guardian2'], {"metadata.broker.list": "localhost:9092"})
    values = kafkaStream.map(lambda x: x[1].encode("ascii", "ignore"))
    lines = values.map(lambda x: x.split("||"))
    # df = lines.toPandas()
    lines.foreachRDD(process)
    # pipeline = Pipeline(stages=[])
    ssc.start()
    ssc.awaitTermination()


def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']


def getSparkSessionInstance(sparkContext):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession(sparkContext)
    return globals()['sparkSessionSingletonInstance']


def process(time, rdd):
    print("========%s===========" % str(time))
    try:
        sqlContext = getSqlContextInstance(rdd.context)
        spark = getSparkSessionInstance(rdd.context)
        df = rdd.toDF()
        pd_df = df.toPandas()
        pd_df.columns = ["label", "review"]
        acc = pipeline(pd_df)
        print(acc)
        sendToES(acc)
    except Exception as e:
        print(e)
        pass

def sendToES(data):
    es = elasticSearch()
    if not es.indices.exists(index="labels"):
        datatype = {
            "mappings": {
                "request-info": {
                    "properties": {
                        "accuracy": {
                            "type": "text"
                        },
                        "output": {
                            "type": "text"
                        },
                        "predicted": {
                            "type": "text"
                        }
                    }
                }
            }
        }
        es.indices.create(index="labels", body=datatype)
    es.index(index="labels", doc_type="request-info", body=data)


if __name__ == "__main__":
    model_building()
    start()
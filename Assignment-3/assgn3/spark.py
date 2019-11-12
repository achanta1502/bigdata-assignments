from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from elasticsearch import Elasticsearch

from utils import model_start
from utils import pipeline

def elastic_search():
    return Elasticsearch([{'host': 'localhost', 'port': 9200}])


def start():

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
    ssc = StreamingContext(sc, 5)
    kafkaStream = KafkaUtils.createDirectStream(ssc, ['guardian2'], {"metadata.broker.list": "localhost:9092"})
    values = kafkaStream.map(lambda x: x[1].encode("ascii", "ignore"))
    lines = values.map(lambda x: x.split("||"))
    lines.foreachRDD(process)
    ssc.start()
    ssc.awaitTermination()


def get_sql_context_instance(sparkContext):
    if 'sqlContextSingletonInstance' not in globals():
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']


def get_spark_session_instance(sparkContext):
    if 'sparkSessionSingletonInstance' not in globals():
        globals()['sparkSessionSingletonInstance'] = SparkSession(sparkContext)
    return globals()['sparkSessionSingletonInstance']


def process(time, rdd):
    print("========%s===========" % str(time))
    try:
        sqlContext = get_sql_context_instance(rdd.context)
        spark = get_spark_session_instance(rdd.context)
        df = rdd.toDF()
        pd_df = df.toPandas()
        pd_df.columns = ["label", "review"]
        acc = pipeline(pd_df)
        print(acc)
        send_to_es(acc)
    except Exception as e:
        print(e)
        pass


def send_to_es(data):
    es = elastic_search()
    if not es.indices.exists(index="labels"):
        datatype = {
            "mappings": {
                "request-info": {
                    "properties": {
                        "accuracy": {
                            "type": "double"
                        },
                        "output": {
                            "type": "long"
                        },
                        "predicted": {
                            "type": "long"
                        }
                    }
                }
            }
        }
        es.indices.create(index="newslabel", body=datatype)
    es.index(index="newslabel", doc_type="request-info", body=data)


if __name__ == "__main__":
    model_start()
    start()
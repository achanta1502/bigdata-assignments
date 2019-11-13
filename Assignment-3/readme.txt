Start zookeeper and kafkaserver
give the fromdate and todate in python file and change the api key inside kafka producer
ex: python stream_producer.py fromdate todate
open the spark folder and run the following command
bin/spark-submit  --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 ~datafolder/spark.py

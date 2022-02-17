from importlib.resources import path
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1'

# initialize spark
from pyspark.sql import SparkSession

polygon_topic = 'polygon-api-output'
news_topic = 'news-api-output'

def spark_init():
    return SparkSession.builder.appName('news_sentiment_analysis').getOrCreate()

def spark_streaming(spark=spark_init()):
    return spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", f'{polygon_topic}, {news_topic}') \
        .load()

def main():
    spark = spark_init()
    spark.conf.set("spark.sql.streaming.checkpointLocation", "../_spark_metadata/checkpoints/")
    spark_stream_reader = spark_streaming()
    spark_stream_writer = spark_stream_reader.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").toDF("key", "value").writeStream.foreach(lambda k, v: print(f"{k}: {v}")).start()

if __name__ == '__main__':
    main()
import pyspark
from pyspark import SparkSession

def spark_init():
    return SparkSession.builder.appName('news_sentiment_analysis').getOrCreate()

def main():
    spark = spark_init()

if __name__ == '__main__':
    main()
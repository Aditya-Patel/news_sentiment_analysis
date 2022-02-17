
from pyspark.sql import SparkSession
import plotly.plotly as py
import plotly.graph_objects as go




def spark_init():
    return SparkSession.builder.appName('news_sentiment_analysis').getOrCreate()


import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from afinn import Afinn


def add_sentiment_score(text):
    afinn = Afinn()
    sentiment_score = afinn.score(text)
    return sentiment_score


if __name__ == "__main__":
    host = "127.0.0.1:9092"
    # port = "9092"
    topic = "tweet_analytics"

    spark = SparkSession \
        .builder \
        .appName("TwitterSentimentAnalysis") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    tweet_rawDF = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", host) \
        .option("startingOffsets", "latest") \
        .option("subscribe", topic) \
        .load()

    tweetDF = tweet_rawDF.selectExpr("CAST(value AS STRING) as tweet")

    sentiment_score_udf = udf(
        add_sentiment_score,
        FloatType()
    )

    tweetDF = tweetDF.withColumn(
        "sentiment_score",
        sentiment_score_udf(tweetDF.tweet)
    )

    query = tweetDF.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="5 seconds") \
        .start() \
        .awaitTermination()

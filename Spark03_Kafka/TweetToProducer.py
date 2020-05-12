import sys
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import json
import pykafka


class TweetsListener(StreamListener):

    def __init__(self, kafkaProducer):

        print("Tweets producer initialized")
        self.producer = kafkaProducer

    def on_data(self, data):

        try:
            json_data = json.loads(data)
            words = json_data["text"].split()
            hashtagList = list(filter(lambda x: x.lower().startswith("#"), words))

            if (len(hashtagList) != 0):
                for hashtag in hashtagList:
                    print(hashtag)
                    self.producer.produce(bytes(hashtag, "utf-8"))

        except KeyError as e:
            print("Error on_data: %s" % str(e))

        return True

    def on_error(self, status):

        print(status)
        return True


def connect_to_twitter(kafkaProducer, tracks):


    auth = OAuthHandler(api_key, api_secret)
    auth.set_access_token(access_token, access_token_secret)

    twitter_stream = Stream(auth, TweetsListener(kafkaProducer))
    twitter_stream.filter(track=tracks, languages=["en"])


if __name__ == "__main__":

    host = "127.0.0.1"
    port = "9092"
    topic = "tweet_analytics"
    tags = "IPL"

    kafkaClient = pykafka.KafkaClient(host + ":" + port)

    kafkaProducer = kafkaClient.topics[bytes(topic, "utf-8")].get_producer()

    connect_to_twitter(kafkaProducer, tags)
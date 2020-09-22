from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import json
import pykafka


class TweetsListener(StreamListener):

    def __init__(self, kafka_producer):
        super().__init__()
        self.producer = kafka_producer
        print("Tweets Producer Initialized")

    def on_data(self, raw_data):
        try:
            json_data = json.loads(raw_data)
            tweet = json_data['text']
            print("\n {} \n".format(tweet))
            self.producer.produce(bytes(json.dumps(tweet), "ascii"))
        except KeyError as e:
            print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        print(status)
        return True


def connect_to_twitter(kafka_producer, track):
    api_key = " "
    api_secret = " "

    access_token = " "
    access_token_secret = " "

    auth = OAuthHandler(api_key, api_secret)
    auth.set_access_token(access_token, access_token_secret)

    twitter_stream = Stream(auth, TweetsListener(kafka_producer))
    twitter_stream.filter(track=track, languages=["en"])


if __name__ == "__main__":
    host = "127.0.0.1"
    port = "9092"
    topic = "tweet_analytics"
    tracks = "IPL"

    kafkaClient = pykafka.KafkaClient(host + ":" + port)

    kafkaProducer = kafkaClient.topics[bytes(topic, "utf-8")].get_producer()

    connect_to_twitter(kafkaProducer, tracks)

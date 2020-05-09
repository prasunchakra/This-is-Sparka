import sys
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener

class TweetsListener(StreamListener):
    def __init__(self):
        print("Tweet listener initialized")

    def on_data(self, raw_data):
        print(raw_data)
        return True

    def on_error(self, status_code):
        print(status_code)
        return True


if __name__ == "__main__":


    auth = OAuthHandler(consumer_key=consumerKey, consumer_secret=consumerSecret)
    auth.set_access_token(key=token, secret=tokenSecret)

    twitter_stream = Stream(auth=auth, listener=TweetsListener())
    twitter_stream.filter(track=['#'])

import sys
import socket
import json
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener


class TweetsListener(StreamListener):
    def __init__(self, client_socket):
        super().__init__()
        self.client_socket = client_socket
        print("Tweet listener initialized")

    def on_data(self, raw_data):
        try:
            json_message = json.loads(raw_data)
            message = json_message['text'].encode('utf-8')
            print(message)
            self.client_socket.send(message)
        except BaseException as be:
            print("Error on data:", str(be))
        return True

    def on_error(self, status_code):
        print(status_code)
        return True


def connect_to_twitter(client_socket):

    auth = OAuthHandler(consumer_key=consumer_key, consumer_secret=consumer_secret)
    auth.set_access_token(key=token, secret=secret)
    twitter_stream = Stream(auth=auth, listener=TweetsListener(client_socket))
    twitter_stream.filter(track=['#'])


if __name__ == "__main__":
    # An endpoint for sending or receiving data on this machine
    s = socket.socket()
    host = "localhost"
    port = 7777
    s.bind((host, port))
    print("Listening on port :%s" % str(port))
    s.listen(5)  # Listen up to 5 connection request

    # A blocking call until a connection to this socket is received
    connection, client_address = s.accept()
    print("Received connection from ", str(client_address))
    connect_to_twitter(connection)


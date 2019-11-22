from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import requests_oauthlib

# Replace the values below with yours
CONSUMER_KEY = 'TmjunbQFYQlKAS1SQczAORHS1'
CONSUMER_SECRET = 'PyGKwMN6xHWXYY1ZAUFIYzyDD4uSfMvJlX8acdKKEmX5gTvM1E'
ACCESS_TOKEN = '1169058393220403206-a3OG28EvzSiPnxNrUYCQrcbmJCzWUy'
ACCESS_SECRET = 'DJ8UYi8ZZbKzKsSxbHtvvXsgmxwfwlWKGTTxjRZJnc7qP'

my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET)


class TweetCollector(StreamListener):

    def __init__(self, client_socket):
        self.client_socket = client_socket

    def on_data(self, json_data):
        try:
            print(json_data)
            self.client_socket.send(str.encode(json_data))
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        print(status)
        return True


def send_data(socket_to_stream):
    auth_config = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth_config.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
    twitter_stream = Stream(auth_config, TweetCollector(socket_to_stream))
    twitter_stream.filter(track=['ghost', 'demon', 'mystery'])


if __name__ == "__main__":
    socket_object = socket.socket()
    socket_object.bind(('localhost', 5656))
    print("Waiting on Port  %s for connections to be established to stream the data" % str(5656))
    socket_object.listen(5)
    connection, address = socket_object.accept()
    print(connection)
    print("Connection request received from client : " + str(address))
    send_data(connection)

import socket
import sys
from thread import start_new_thread
import thread
from tweepy.streaming import StreamListener, json
from tweepy import OAuthHandler
from tweepy import Stream
from yaml import load, dump
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

class PyStreamer(object):

    #Variables that contains the user credentials to access Twitter API
    with open("credentials.yaml", "r") as file:
        cred = load(file, Loader=Loader)

    consumer_key = cred["consumer_key"]
    consumer_secret = cred["consumer_secret"]
    access_token = cred["access_token"]
    access_token_secret = cred["access_token_secret"]

    HOST = ''    # Symbolic name meaning all available interfaces
    PORT = 9999  # Arbitrary non-privileged port

    def __init__(self, languages, topics):
        self.socket_created = False
        start_new_thread(self.handle_socket, ())
        self.should_end = False
        self.languages = languages
        self.topics = topics
        while not self.socket_created:
            pass


    def handle_socket(self):

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        print 'Socket created'
        try:
            s.bind((self.HOST, self.PORT))
        except socket.error , msg:
            print 'Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
            sys.exit()

        self.socket_created = True

        print 'Socket bind complete'
        s.listen(10)
        print 'Socket now listening'

        # This is a basic listener that just prints received tweets to stdout.
        class StdOutListener(StreamListener):

            def on_data(self, data):
                # post = json.loads(data.decode('utf-8'))
                conn.send(data)
                return True

            def on_error(self, status):
                print status

        #now keep talking with the client
        while not self.should_end:
            #wait to accept a connection - blocking call
            conn, addr = s.accept()
            print 'Connected with ' + addr[0] + ':' + str(addr[1])

            listener = StdOutListener()
            auth = OAuthHandler(self.consumer_key, self.consumer_secret)
            auth.set_access_token(self.access_token, self.access_token_secret)
            stream = Stream(auth, listener)

            # This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
            stream.filter(track=self.topics, languages=self.languages)

        s.close()
        thread.exit()
#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

#Variables that contains the user credentials to access Twitter API
access_number = "86457010-pJp5znqBdUIm8pvspVYfxUyfYarmh1WwkFAfHrWMX"
access_number_sec = "5c3Au47cUJvi0OnDAj7YeQUBDbLjGvXo8l7PHFqnLHwhP"
my_key = "dF7iP9i3cxpcLeDqAaoaBjjvT"
my_secret = "XzxL2WoE2dCTOZNAGnmXI1w4VYgyJNhO4t02DjeK7mEq2M4D4w"


#This is a basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):

    def on_data(self, data):
        print (data)
        return True

    def on_error(self, status):
        print (status)


if __name__ == '__main__':

    #This handles Twitter authentication and the connection to Twitter Streaming API
    l = StdOutListener()
    authorization = OAuthHandler(my_key, my_secret)
    authorization.set_access_token(access_number, access_number_sec)
    stream = Stream(authorization, l)

    #This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
    stream.filter(track=['android'])
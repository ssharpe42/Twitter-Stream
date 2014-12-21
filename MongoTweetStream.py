import time
import json
from textblob import TextBlob
from datetime import datetime
import pymongo
import time

#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

#Variables that contains the user credentials to access Twitter API 
ckey = ''
csecret = ''
atoken = ''
asecret = ''


class Streamer(StreamListener):

	INCLUDED_FIELDS = ['text','timestamp_ms','sentiment', 'subjectivity', 'year', 'month','day','hour','minute','positive','negative','neutral']

	def __init__(self, keywords, db, maxtime):
		self.db = db
		self.keywords = keywords
		self.time = datetime.now()
		self.maxtime = maxtime
	def on_data(self, data):
		
		polarity = TextBlob(json.loads(data)["text"]).translate(to='en').sentiment.polarity
		subjectivity = TextBlob(json.loads(data)["text"]).translate(to='en').sentiment.subjectivity
		tweet = json.loads(data)
		tweet['sentiment'] = polarity
		tweet['subjectivity'] = subjectivity
		tweet['positive'] = 1 if polarity > 0 else 0
		tweet['negative'] = 1 if polarity < 0 else 0
		tweet['neutral'] = 1 if polarity == 0 else 0
		date = datetime.fromtimestamp(float(json.loads(data)['timestamp_ms'].encode('ascii', 'ignore'))/1000)
		tweet['timestamp_ms'] = int(tweet['timestamp_ms'])
		tweet['year'] = date.year
		tweet['month'] = date.month
		tweet['day'] = date.day
		tweet['hour'] = date.hour
		tweet['minute'] = date.minute
		short_tweet = dict(zip(self.INCLUDED_FIELDS,[tweet[x] for x in self.INCLUDED_FIELDS]))
		#For each keyword, check if it is in the tweet, if it is add it to the db instance with the keyword label
		for word in self.keywords:
			if word.lower() in short_tweet['text'].lower():
				T = short_tweet
				T['keyword'] = word

				try:
					self.db.insert(T)
					print "Tweet for keyword " + word + " inserted: " + T['text'].encode('ascii', 'ignore')
				except pymongo.errors.DuplicateKeyError:
					print "Duplicate Key Error. Insertion of Tweet Failed"
		#If we have run it for the maximum amount of time then we exit out of the stream
		if (datetime.now()-self.time).total_seconds()/60 > self.maxtime:
			print 'Exited after ' + str((datetime.now()-self.time).total_seconds()/60) + ' minutes'
			return False
		#dont break stream
		else:
			return True

	def on_error(self, status):
			print 'error'

def tweet_stream(keywords, db, maxtime):
	#This handles Twitter authetification and the connection to Twitter Streaming API
	try:
		l = Streamer(keywords, db, maxtime)
		auth = OAuthHandler(ckey, csecret)
		auth.set_access_token(atoken, asecret)
		stream = Stream(auth, l, language='en')
		#This line filter Twitter Streams to capture data by the keyword
		stream.filter(track=keywords)
	except:
		tweet_stream(keywords, db, maxtime)
	
if __name__ == '__main__':

	connection = pymongo.Connection("mongodb://localhost", safe=True)
	db=connection.twitter
	twitter_stream = db.stream
	tweet_stream(['nbcsvu','sportscenter','gameofthrones','breakingbad_AMC'], twitter_stream, 5*60)









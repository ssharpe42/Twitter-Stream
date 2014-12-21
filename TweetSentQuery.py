
import pymongo
import json
import datetime as d
import pprint

def open_connection():
	try: 
		connection = pymongo.Connection("mongodb://localhost", safe=True)
		db=connection.twitter
		twitter_stream = db.stream
		return twitter_stream
	except pymongo.errors.ConnectionFailure:
		print "There seems to be a problem connecting to the mongodb instance"

def indexes(twitter_stream):
#Make sure all proper indexes are present on the database
	twitter_stream.ensure_index("keyword")
	twitter_stream.ensure_index("timestamp_ms")
	twitter_stream.ensure_index([("year", pymongo.DESCENDING), ("month", pymongo.DESCENDING), ("day", pymongo.DESCENDING)])
	twitter_stream.ensure_index([("month", pymongo.DESCENDING), ("day", pymongo.DESCENDING)])
	twitter_stream.ensure_index([("day", pymongo.DESCENDING)])

#create the group by criterion and the date filters based on the keyword, groupby arrays, start and end dates, 
#and the boolean whether to include keyword in the group by ID
def group_date_creation(keywords, groupby, ID_keyword,start_date, end_date):
	if ID_keyword:
		groups = "{ 'keyword':'$keyword', "
		for g in sorted(groupby, reverse=True):
			groups += "'" + g +"'" +":'$" + g + "'" ', '
		groups = groups[:-2] + ' }'
		groups = eval(groups)
	elif groupby!=[] and ID_keyword:
		groups = '{'
		for g in sorted(groupby, reverse=True):
			groups += "'" + g +"'" +":'$" + g + "'" ', '
		groups = groups[:-2] + ' }'
		groups = eval(groups)
	else:
		groups = 'null'
	start = start_date.split('-')
	end  = end_date.split('-')
	start_month = int(start[0])
	start_day = int(start[1])
	start_year = int(start[2])
	end_month = int(end[0])
	end_day = int(end[1])
	end_year = int(end[2])
	start_milli = (d.datetime(start_year,start_month,start_day,0,0,0) - d.datetime.fromtimestamp(0)).total_seconds()*1000
	end_milli = (d.datetime(end_year,end_month,end_day,23,59,59) - d.datetime.fromtimestamp(0)).total_seconds()*1000

	return groups, start_milli, end_milli	

#Returns average sentiment (and subjectivity) for a certain query
def average_sentiment(db, start_date, end_date, groupby = [], keywords = [],ID_keyword = True):
	
	keywords = [w.lower() for w in keywords]
	groupby = [w.lower() for w in groupby]
	
	groups, start_milli, end_milli = group_date_creation(keywords, groupby, ID_keyword,start_date, end_date)

	if keywords != []:
		pipeline = [
			{'$match': {
				'keyword': {'$in': keywords},
				'timestamp_ms': {'$gt': start_milli,'$lt': end_milli}
			}}, 
			{'$group':
				{
				'_id': groups,
				'avg_sentiment': {'$avg':'$sentiment'},
				'avg_subjectivity': {'$avg':'$subjectivity'}
				}
			}
		]
	elif keywords ==[]:
		pipeline = [
			{'$match': {
				'timestamp_ms': {'$gt': start_milli,'$lt': end_milli}
			}}, 
			{'$group':
				{
				'_id': groups,
				'avg_sentiment': {'$avg':'$sentiment'},
				'avg_subjectivity': {'$avg':'$subjectivity'}
				}
			}
		]
	return db.aggregate(pipeline=pipeline)['result']

#Returns percent positive, negative, and neutral tweets for a certain query
def percent_sentiment( db, start_date, end_date, groupby = [], keywords = [],ID_keyword = True):
	
	keywords = [w.lower() for w in keywords]
	groupby = [w.lower() for w in groupby]
	groups, start_milli, end_milli = group_date_creation(keywords, groupby, ID_keyword,start_date, end_date)

	if keywords != []:
		pipeline = [
			{'$match': {
				'keyword': {'$in': keywords},
				'timestamp_ms': {'$gte': start_milli,'$lte': end_milli}
			}}, 
			{'$group':
				{
				'_id': groups,
				'positive': {'$avg':'$positive'},
				'negtive': {'$avg':'$negative'},
				'neutral': {'$avg':'$neutral'}
				}
			}
		]
	elif keywords ==[]:
		pipeline = [
			{'$match': {
				'timestamp_ms': {'$gte': start_milli,'$lte': end_milli}
			}}, 
			{'$group':
				{
				'_id': groups,
				'positive': {'$avg':'$positive'},
				'negtive': {'$avg':'$negative'},
				'neutral': {'$avg':'$neutral'}
				}
			}
		]
	return db.aggregate(pipeline=pipeline)['result']

#Returns volume of tweets for a certain query
def num_tweets(db, start_date, end_date, groupby = [], keywords = [], ID_keyword = True):
	
	keywords = [w.lower() for w in keywords]
	groupby = [w.lower() for w in groupby]
	groups, start_milli, end_milli = group_date_creation(keywords, groupby, ID_keyword,start_date, end_date)

	if keywords != []:
		pipeline = [
			{'$match': {
				'keyword': {'$in': keywords},
				'timestamp_ms': {'$gte': start_milli,'$lte': end_milli}
			}}, 
			{'$group':
				{
				'_id': groups,
				'count': {'$sum':1}
				}
			}
		]
	elif keywords ==[]:
		pipeline = [
			{'$match': {
				'timestamp_ms': {'$gte': start_milli,'$lte': end_milli}
			}}, 
			{'$group':
				{
				'_id': groups,
				'count': {'$sum':1}
				}
			}
		]
	return db.aggregate(pipeline=pipeline)['result']



if __name__ == '__main__':
	#examples
	twitter_stream = open_connection()
	indexes(twitter_stream)
	a = average_sentiment(db = twitter_stream, start_date = '1-2-2014',end_date = '12-25-2014', groupby = [], keywords = [], ID_keyword = False)
	print pprint.pprint(a)
	b = percent_sentiment(db = twitter_stream, start_date = '1-2-2014',end_date = '12-25-2014', groupby = [], keywords = [], ID_keyword = True)
	print pprint.pprint(b)
	c = num_tweets(db = twitter_stream, start_date = '1-2-2014',end_date = '12-25-2014', groupby = [], keywords = [],ID_keyword = False)
	print pprint.pprint(c)

#!/usr/bin/env python
import got3 as got
import argparse
from elasticsearch import Elasticsearch

## Configure argparse
parser = argparse.ArgumentParser(
  prog="get-tweet.py",
  description='Get tweet and import Elasticsearch.'
  )
  
parser.add_argument("-f", "--firstdate",   type=str, required=True,       help='<Require> First tweet date "YYYY-MM-DD".')
parser.add_argument("-l", "--lastdate",    type=str, required=True,       help='<Require> Last tweet date "YYYY-MM-DD".')
parser.add_argument("-t", "--text",        type=str, required=True,       help='<Require> Search text "xxxxx".')
parser.add_argument("-c", "--count",       type=int, default=3000,        help='<Option> Maximum number to collect "N", default 3000.')
parser.add_argument("-e", "--exporthost",  type=str, default="localhost", help='<Option> Destination elasticsearch host, default localhost.')
parser.add_argument("-p", "--exportport",  type=int, default=9200,        help='<Option> Destination elasticsearch port, default 9200.')

args       = parser.parse_args()
firstdate  = args.firstdate
lastdate   = args.lastdate
text       = args.text
count      = args.count
exporthost = args.exporthost
exportport = args.exportport

## Configure elasticsearch host
es = Elasticsearch("{}:{}".format(exporthost, exportport))

## Retrieve tweets
tweetCriteria = got.manager.TweetCriteria().setQuerySearch(text).setSince(firstdate).setUntil(lastdate).setMaxTweets(count)

## Export tweet to Elasticsearch

for tweet in got.manager.TweetManager.getTweets(tweetCriteria):
    indexname = "bigdata-{}".format(str(tweet.date)[0:4])
    es.index(index=indexname, doc_type="tweet", id=tweet.id, body={
        "username":tweet.permalink.split('/')[3],
        "text":tweet.text,
        "timestamp":tweet.date,
        "retweets":tweet.retweets,
        "favorites":tweet.favorites
    })

## Finish message
print("Finish retrieve tweet {} to {}".format(firstdate, lastdate)+". Total "+str(len(got.manager.TweetManager.getTweets(tweetCriteria)))+"tweet")

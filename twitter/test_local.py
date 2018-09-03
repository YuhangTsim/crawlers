"""
Crawler to get healthcare tweets.
"""
# import re
import logging
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from twitterCrawler import twitterCrawler


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    tc = twitterCrawler()
    tweet = tc.get_tweet()
    count = 0

    client = MongoClient('localhost', 27017)
    db = client.twitter

    while count < 20:
        try:
            info = tweet.__next__()
            if info:
                db.detail.insert_one(info)
                count += 1
                logging.info('---- Insert Id %s', info['_id'])
        except DuplicateKeyError:
            logging.info('---- Duplicate key for %s', info['_id'])
        except Exception as e:
            logging.error('---- Error : %s', e)
    client.close()
    logging.info('Done, %s inserted', count)

"""
scriptt to fetch new tweets
"""
import re
import logging
from sys import argv
import MySQLdb
from twitterscraper_modified.twitterscraper.query import *

dbconfig = {
    'host': '142.93.125.178',
    'user': 'root',
    'pw': 'j3ss3',
    'db': 'twitter'
}

sql_ignore = """
INSERT IGNORE INTO
healthcareTweet
    (tweetId, content, tweetTime, userName, reply, retweet, likeCount, kw)
Values
("{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}")
"""

if __name__ == '__main__':
    logging.basicConfig(
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        level=logging.INFO)

    try:
        limit = int(argv[1]) if len(argv) == 2 else 20
    except:
        limit = None
        search_kw = argv[1]

    if len(argv) >= 3:
        search_kw = argv[2]
    elif limit:
        search_kw = 'healthcare'

    count = 0
    query_limit = 0

    def db_insert(tweets):
        """
        Insert tweet into db
        """
        try:
            info = [(tweet.id, re.sub('\"', '\'', tweet.text),
                     tweet.timestamp, tweet.user, tweet.replies, tweet.retweets,
                     tweet.likes, search_kw) for tweet in tweets]
            args = ['%s'] * 8
            sql = sql_ignore.format(*args)
            cursor.executemany(sql, info)
            conn.commit()
            logging.info('---- %s insert', len(tweets))
        except Exception as e:
            logging.warning('---- Warning insert: %s', e)
            conn.rollback()

    if limit and limit > 20000 and False:
        while count <= limit:
            query_limit += limit
            tweets = query_tweets(
                search_kw, limit=query_limit, db=db_insert)
    else:
        tweets = query_tweets(search_kw)
        conn = MySQLdb.connect(
            host=dbconfig['host'],
            user=dbconfig['user'],
            password=dbconfig['pw'],
            db=dbconfig['db'],
            charset="utf8")
        cursor = conn.cursor()
        for _, tweet in enumerate(tweets):
            info = [tweet.id, tweet.text, tweet.timestamp, tweet.user,
                    tweet.replies, tweet.retweets, tweet.likes, search_kw]
            try:
                cursor.execute(sql_ignore.format(*info))
                conn.commit()
                count += 1
                logging.info('---- %s / %s', _, len(tweets))
            except Exception as e:
                logging.warning('---- Warning : %s', e)
                conn.rollback()

    conn.close()
    logging.info('---- Done')

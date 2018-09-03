"""
Crawler to get healthcare tweets.
"""
import re
import time
from sys import argv
import logging
import warnings
import traceback
import MySQLdb
from twitterCrawler import twitterCrawler


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    warnings.filterwarnings('error', category=MySQLdb.Warning)

    tc = twitterCrawler()
    tweet = tc.get_tweet()
    count = 0
    total = 0

    dbconfig = {
        'host': '****',
        'user': 'root',
        'pw': '****',
        'db': 'twitter'
    }

    conn = MySQLdb.connect(
        host=dbconfig['host'],
        user=dbconfig['user'],
        password=dbconfig['pw'],
        db=dbconfig['db'],
        charset="utf8")
    cursor = conn.cursor()
    sql_update = """
    INSERT INTO
    healthcareTweet
        (tweetId, content, tweetTime, userName, reply, retweet, likeCount)
    Values
    ("{}", "{}", "{}", "{}", "{}", "{}", "{}")
    ON DUPLICATE KEY UPDATE content = VALUES(content)
    """

    sql_ignore = """
    INSERT IGNORE INTO
    healthcareTweet
        (tweetId, content, tweetTime, userName, reply, retweet, likeCount)
    Values
    ("{}", "{}", "{}", "{}", "{}", "{}", "{}")
    """

    limit = int(argv[1]) if len(argv) == 2 else 20

    while count < limit:
        try:
            info = tweet.__next__()
            if info:
                info['content'] = re.sub('—', '-', info['content'])
                info['content'] = re.sub('’', '\'', info['content'])
                info['content'] = re.sub('"', '\'', info['content'])
                # info['content'] = re.sub('[\u2010-\u2050]', '?', info['content'])
                execute_sql = sql_ignore.format(*(info.values()))
                r = cursor.execute(execute_sql)
                conn.commit()
                logging.info('---- Insert Id %s', info['_id'])
                count += 1
        except Warning as w:
            logging.debug('---- Warning: %s', w)
        except Exception as e:
            logging.error('---- Error : %s \n', e)
            logging.error('Track: %s \n SQL: %s',
                          traceback.format_exc(), execute_sql)
        total += 1
    conn.close()
    logging.info('Done, %s inserted', count)

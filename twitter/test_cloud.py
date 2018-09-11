"""
Crawler to get healthcare tweets.
"""
import re
from sys import argv
import logging
import warnings
import traceback
import MySQLdb
from twitterSeleniumCrawler import twitterCrawler


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    warnings.filterwarnings('error', category=MySQLdb.Warning)

    count = 0
    total = 0

    dbconfig = {
        'host': '142.93.125.178',
        'user': 'root',
        'pw': 'j3ss3',
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
        (tweetId, content, tweetTime, userName, reply, retweet, likeCount, kw)
    Values
    ("{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}")
    """

    limit = int(argv[1]) if len(argv) >= 2 else 20
    if len(argv) >= 3:
        tc = twitterCrawler(search_kw=argv[2])
    else:
        tc = twitterCrawler()
    tweet = tc.get_tweet()

    while count < limit:
        try:
            info = tweet.__next__()
            if info:
                info['content'] = re.sub('—', '-', info['content'])
                info['content'] = re.sub('’', '\'', info['content'])
                info['content'] = re.sub('"', '\'', info['content'])
                # info['content'] = re.sub('[\u2010-\u2050]', '?', info['content'])
                info['kw'] = tc.search_kw
                execute_sql = sql_ignore.format(*(info.values()))
                r = cursor.execute(execute_sql)
                logging.info('---- Insert Id %s', info['_id'])
                conn.commit()
                count += 1
        except Warning as w:
            logging.debug('---- Warning: %s', w)
        except Exception as e:
            logging.error('---- Error : %s \n', e)
            logging.error('Track: %s \n SQL: %s',
                          traceback.format_exc(), execute_sql)
        total += 1
    conn.close()
    logging.info('Done, %s inserted, %s in totall fetched', count, total)

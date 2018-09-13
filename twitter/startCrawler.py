"""
Start job, crawling
"""
from sys import argv
from utilies.mysqlConnection import mysqlconnector
from twitterCrawler.twitterCrawler import *

dbconfig = {
    'host': '142.93.125.178',
    'user': 'root',
    'pw': 'j3ss3',
    'db': 'twitter'
}

if __name__ == '__main__':
    mysqldb = mysqlconnector(
        usr=dbconfig['user'],
        pw=dbconfig['pw'],
        db=dbconfig['db'],
        host=dbconfig['host'])

    if len(argv) == 2:
        kw = argv[1]
        limit = None
    elif len(argv) == 3:
        kw = argv[1]
        limit = int(argv[2])

    if kw and limit:
        tweets = query_tweet(kw, limit=limit, db=True, poolsize=10)
    elif kw:
        tweets = query_tweet(kw, db=True, poolsize=20)

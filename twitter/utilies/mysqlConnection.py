"""
Implementing class for db insertion
"""

import os
import logging
import MySQLdb


class mysqlconnector():
    """
    DB insertion

    usr, pw, db, host, port=3306, search=None
    """

    def __init__(self, usr, pw, db, host, port=3306, search_kw=None):
        newpath = './log'
        if not os.path.exists(newpath):
            os.makedirs(newpath)
        self.logger = logging.getLogger('MySQL_conn')
        formatter = logging.Formatter(
            '-%(asctime)-15s-%(name)s-%(levelname)s: %(message)s')
        handler1 = logging.StreamHandler()
        handler2 = logging.FileHandler('./log/db.log')
        handler1.setFormatter(formatter)
        handler2.setFormatter(formatter)
        self.logger.addHandler(handler1)
        self.logger.addHandler(handler2)
        self.logger.setLevel(logging.INFO)

        self.usr = usr
        self.pw = pw
        self.db = db
        self.port = port
        self.host = host
        self.search_kw = search_kw

        self.connection = False

        self.sql_ignore = """
        INSERT IGNORE INTO
        healthcareTweet
            (tweetId, content, tweetTime, userName, reply, retweet, likeCount, kw)
        Values
        ("{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}")
        """

    def insert(self, Tweets, num=10000):
        """
        Tweets Insertion
        """
        if not self.connection:
            self.connect()
        if not Tweets:
            return
        if not isinstance(Tweets, list):
            self.logger.error('---- Tweets list Type error')
            return

        elif len(Tweets) == 1:
            info = [(tweet.id, tweet.text,
                     tweet.timestamp, tweet.user, tweet.replies, tweet.retweets,
                     tweet.likes, self.search_kw) for tweet in Tweets]
            sql = self.sql_ignore.format(*info)
            try:
                self.cursor.execute(sql)
                self.conn.commit()
            except Exception as e:
                self.logger.exception('---- Exception: %s, %s', e, info)
                self.conn.rollback()
        else:
            formatter = ['%s'] * 8
            sql = self.sql_ignore.format(*formatter)
            for i in range(0, len(Tweets)+1, num):
                try:
                    info = [(tweet.id, tweet.text,
                             tweet.timestamp, tweet.user, tweet.replies, tweet.retweets,
                             tweet.likes, self.search_kw) for tweet in Tweets[i:i+num]]
                    self.cursor.executemany(sql, info)
                    self.conn.commit()
                    inserted_num = i+num if len(Tweets) > num else len(Tweets)
                    self.logger.info(
                        '---- Insertion {}/{}'.format(
                            inserted_num
                            if inserted_num <= len(Tweets) else len(Tweets),
                            len(Tweets)))
                except (AttributeError, MySQLdb.OperationalError):
                    self.connect()
                    self.insert(Tweets)
                except Exception as e:
                    self.conn.rollback()
                    self.logger.exception('---- Exception: %s', e)

    def connect(self):
        """
        create connection and cursor
        """
        self.conn = MySQLdb.connect(
            host=self.host, user=self.usr, passwd=self.pw, db=self.db,
            port=self.port, charset="utf8")
        self.cursor = self.conn.cursor()
        self.connection = True
        self.logger.debug('DB connection open')

    def test(self, sql):
        """
        Testing class
        """
        if not self.connection:
            self.connect()
        try:
            self.cursor.execute(sql)
            self.conn.rollback()
            print('Success.')
        except Exception as e:
            print(f'Error {e}')

    def __del__(self):
        if self.connection:
            self.conn.close()
            self.logger.debug('Connection closed')


if __name__ == '__main__':
    dbconfig = {
        'host': '142.93.125.178',
        'user': 'root',
        'pw': 'j3ss3',
        'db': 'twitter'
    }
    mysqldb = mysqlconnector(
        usr=dbconfig['user'],
        pw=dbconfig['pw'],
        db=dbconfig['db'],
        host=dbconfig['host'])
    mysqldb.test('Select count(*) from healthcareTweet')

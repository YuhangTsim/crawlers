"""
test crawler
insert into db
"""

import logging
import time
from sys import argv
import MySQLdb
from pymongo import MongoClient
from Redfin import Redfin

dbconfig = {
    'host': 'localhost',
    'user': 'root',
    'pw': '9025',
    'db': 'redfin'
}


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    crawler = Redfin()
    conn = MySQLdb.connect(
        host=dbconfig['host'],
        user=dbconfig['user'],
        passwd=dbconfig['pw'],
        db=dbconfig['db'])
    cursor = conn.cursor()
    sql = """
    Select property_id, address
    From properties
    """
    cursor.execute(sql)
    addresses = {i[0]: i[1] for i in cursor.fetchall()}
    conn.close()
    client = MongoClient('localhost', 27017)
    db = client.redfin

    exist_id = set([record['_id'] for record in db.detail.find({})])
    useless_id = set([record['_id'] for record in db.useless.find({})])
    if len(argv) > 1:
        count = int(argv[1])
    else:
        count = len(addresses)
    logging.info('---- %s properties to go', count)
    logging.info('---- %s useless id and %s crawled',
                 len(useless_id), len(exist_id))
    crawled_cnt = 0
    for i, address in addresses.items():
        if i not in exist_id and i not in useless_id and crawled_cnt <= count:
            try:
                logging.debug('---- Getting id: %s', i)
                url = crawler.search(address, url=True)
                if not url:
                    db.useless.insert_one(
                        {'_id': i, 'reason': 'No result', 'address': address})
                    time.sleep(0.2)
                    continue
                detail = crawler.parse()
                record = {
                    '_id': i,
                    'url': url,
                    'detail': detail
                }
                db.detail.insert_one(record)
                crawled_cnt += 1
                time.sleep(0.5)
            except Exception as e:
                logging.error('---- Error : %s', e)
            finally:
                if (crawled_cnt > 0)and (crawled_cnt % 20) == 0:
                    time.sleep(20)
                    logging.info(
                        '---- Sleep 20s ---- %s pages crawled', crawled_cnt)
    client.close()

"""
test crawler
insert into db
"""

import logging
import MySQLdb
import time
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
    c = Redfin()
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
    addresses = {i[0]: i[1] for i in cursor.fetchall()[:100]}
    client = MongoClient('localhost', 27017)
    db = client.redfin

    for i in addresses:
        try:
            logging.info('---- Getting id: %s', i)
            url = c.search(addresses[i], url=True)
            detail = c.parse()
            record = {
                '_id': i,
                'url': url,
                'detail': detail
            }
            db.detail.insert_one(record)
        except Exception as e:
            logging.error('---- Error : %s', e)
        finally:
            time.sleep(0.2)
    conn.close()

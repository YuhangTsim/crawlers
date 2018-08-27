"""
Demo to crawl twitter
"""
# import re
import logging
import time
from datetime import datetime
from selenium import webdriver
from pymongo import MongoClient


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    chrome_option = webdriver.ChromeOptions()
    chrome_option.add_argument('--disable-gpu')
    chrome_option.add_argument('--headless')
    driver = webdriver.Chrome(chrome_options=chrome_option)
    driver.implicitly_wait(5)

    client = MongoClient('localhost', 27017)
    db = client.twitter

    driver.get('https://twitter.com/search?q=healthcare&src=tyah')
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(1)

    tweets = driver.find_elements_by_xpath(
        '//ol[1]/li[@data-item-type="tweet"]')
    for i, tweet in enumerate(tweets):
        info = {}
        try:
            info['_id'] = tweet.get_attribute('data-item-id')
            content = tweet.find_element_by_xpath(
                '//ol[1]/li[@data-item-type="tweet"][%d]//div[@class="js-tweet-text-container"]' % (i+1))
            info['content'] = content.text
            info['time'] = datetime.fromtimestamp(int(tweet.find_element_by_xpath(
                '//ol[1]/li[@data-item-type="tweet"][%d]//span[@data-time-ms]' % (i+1)).get_attribute('data-time-ms')[:-3])).strftime('%Y-%m-%d %H:%M:%S')
            info['user'] = tweet.find_element_by_xpath(
                '//ol[1]/li[@data-item-type="tweet"][%d]//span[@class="username u-dir u-textTruncate"and@data-aria-label-part]/b' % (i+1)).text
            db.tweets.insert_one(info)
            logging.info('---- %s Inserted', info['_id'])
        except Exception as e:
            logging.error('---- Error : %s', e)
    driver.close()
    client.close()
    logging.info('Done')

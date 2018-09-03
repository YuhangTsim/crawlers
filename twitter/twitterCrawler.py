"""
Class to crawl tweets by searchfrom twitter.com
"""
import logging
import time
import random
from datetime import datetime
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException


class twitterCrawler():
    """
    Crawler class
    """

    def __init__(self, search_kw='healthcare', wait=2, scroll=2):
        self.log = logging.getLogger('Healthcare_crawler')
        chrome_option = webdriver.ChromeOptions()
        chrome_option.add_argument('--disable-gpu')
        chrome_option.add_argument('--headless')
        self.driver = webdriver.Chrome(chrome_options=chrome_option)
        self.status = True
        self.driver.implicitly_wait(wait)
        self.scroll = scroll
        self.search_kw = search_kw
        self.log.info('---- Driver initialized, with keyword: %s',
                      self.search_kw)
        self.driver.get(
            f'https://twitter.com/search?q={self.search_kw}&src=tyah')

    def __del__(self):
        self.driver.close()
        self.log.debug('---- Driver closed')

    def reconnect(self):
        """
        close driver
        """
        if self.status:
            self.driver.close()
            self.log.info('---- Driver closed')

    def set_search_kw(self, search_kw='healthcare'):
        """
        Set search keywords.
        """
        self.search_kw = search_kw
        self.driver.get(
            f'https://twitter.com/search?q={self.search_kw}&src=tyah')
        self.log.info('---- Search kw change to %s', self.search_kw)

    def get_tweet(self):
        """
        Get tweets.
        """
        self.log.info('---- Fetching Tweets')
        count = 0
        scroll_history = 0
        droplist = []
        successlist = []
        while True:
            tweets = self.driver.find_elements_by_xpath(
                "//li[@class=\"js-stream-item stream-item stream-item\n\"]")
            for idx, tweet in enumerate(tweets):
                info = {}
                if tweet not in droplist and tweet not in successlist:
                    try:
                        _id = tweet.get_attribute('data-item-id')
                        info['_id'] = _id
                        content = tweet.find_element_by_class_name(
                            "js-tweet-text-container")
                        info['content'] = content.text
                        info['time'] = datetime.fromtimestamp(int(tweet.find_element_by_xpath(
                            f'//li[@data-item-id="{_id}"]//span[@data-time]').get_attribute('data-time'))).strftime('%Y-%m-%d %H:%M:%S')
                        info['user'] = tweet.find_element_by_xpath(
                            f'//li[@data-item-id="{_id}"]//span[@class="username u-dir u-textTruncate"]/b').text
                        try:
                            numbers = tweet.find_elements_by_class_name(
                                'ProfileTweet-actionCountForPresentation')
                            info['#reply'] = numbers[0].text
                            info['#retweet'] = numbers[1].text
                            info['#like'] = numbers[3].text
                        except NoSuchElementException:
                            logging.debug(
                                '---- No #reply or #retweet or #like for id %s',
                                info['_id'])
                            info['#reply'], info['#retweet'], info['#like'] = [
                                'Null'] * 3
                        if '_id' in info and 'content' in info:
                            count += 1
                            successlist.append(tweet)
                            yield info
                    except NoSuchElementException:
                        self.log.info(
                            '---- Fail to get basic elements')
                        droplist.append(tweet)
                        yield None
                    except Exception as e:
                        self.log.error('---- Error : %s', e)
                        droplist.append(tweet)
                        yield None
            if scroll_history % 11 == 1:
                self.driver.get(
                    f'https://twitter.com/search?q={self.search_kw}&src=tyah')
            for _ in range(self.scroll):
                self.driver.execute_script(
                    "window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(random.random()*2)
                scroll_history += 1


if __name__ == '__main__':
    tc = twitterCrawler()
    tweet = tc.get_tweet()
    for _ in range(5):
        i = tweet.__next__()
        if i:
            print(
                f'Get twitter {i}')
    print('Success.')

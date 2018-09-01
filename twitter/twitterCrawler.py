"""
Class to crawl tweets by searchfrom twitter.com
"""
import logging
import time
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

    def close(self):
        """
        close driver
        """
        if self.status:
            self.driver.close()
            self.log.info('---- Driver closed')

    def set_search_kw(self, search_kw):
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
        droplist = []
        while True:
            tweets = self.driver.find_elements_by_xpath(
                '//ol[1]/li[@data-item-type="tweet"]')
            for idx, tweet in enumerate(tweets):
                info = {}
                if tweet not in droplist:
                    try:
                        info['_id'] = tweet.get_attribute('data-item-id')
                        content = tweet.find_element_by_xpath(
                            '//ol[1]/li[@data-item-type="tweet"][%d]//div[@class="js-tweet-text-container"]' % (idx+1))
                        info['content'] = content.text
                        info['time'] = datetime.fromtimestamp(
                            int(
                                tweet.find_element_by_xpath(
                                    '//ol[1]/li[@data-item-type="tweet"][%d]//span[@data-time-ms]'
                                    % (idx + 1)).get_attribute('data-time-ms')
                                [: -3])).strftime('%Y-%m-%d %H:%M:%S')
                        info['user'] = tweet.find_element_by_xpath(
                            '//ol[1]/li[@data-item-type="tweet"][%d]//span[@class="username u-dir u-textTruncate"and@data-aria-label-part]/b' % (idx+1)).text
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
                            yield info
                    except NoSuchElementException:
                        self.log.info(
                            '---- Fail to get basic elements')
                        droplist.append(tweet)
                        yield None
                    except Exception as e:
                        self.log.error('---- Error : %s', e)
                        yield None
            for _ in range(self.scroll):
                self.driver.execute_script(
                    "window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)


if __name__ == '__main__':
    tc = twitterCrawler()
    tweet = tc.get_tweet()
    for _ in range(5):
        i = tweet.__next__()
        if i:
            print(
                f'Get twitter id :{i["_id"]}, from {i["user"]}, tweet at {i["time"]}')
    print('Success.')

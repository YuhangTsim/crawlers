"""
test file to crawl redfin.com
"""
import logging
from selenium import webdriver
from scrapy.selector import Selector
from selenium.common.exceptions import NoSuchElementException


class Redfin():
    """
    Redfin crawler
    """
    detail_statu = False

    def __init__(self, wait=2):
        chrome_option = webdriver.ChromeOptions()
        chrome_option.add_argument('--headless')
        chrome_option.add_argument('--disable-gpu')
        self.driver = webdriver.Chrome(chrome_options=chrome_option)
        self.driver.implicitly_wait(wait)

    def __del__(self):
        self.driver.close()

    def search(self, address='', url=True):
        """
        Search property on redfin
        """
        baseurl = 'https://www.redfin.com/'
        try:
            self.driver.get(baseurl)
            if not address:
                print(f'---- testing {self.driver.current_url}')
                return None
            search_input = self.driver.find_element_by_xpath(
                '//input[@type="search"]')
            search_input.send_keys(address)
            search_btn = self.driver.find_element_by_xpath(
                '//button[@data-rf-test-name="searchButton"]')
            search_btn.click()
            self.driver.find_element_by_xpath(
                '//span[@itemprop="streetAddress"]')
            result = self.driver.current_url
            self.detail_statu = True
            logging.debug('---- Property page : %s', result)
            if url:
                return result
        except NoSuchElementException as e:
            logging.info('---- No such element')
        except Exception as e:
            logging.error('---- Error : %s', e)
            result = 'None'
            if url:
                return result

    def parse(self):
        """
        Get facts
        """
        result = {}
        if self.detail_statu:
            sel = Selector(text=self.driver.page_source)

            fact_table = sel.xpath(
                '//div[@class="facts-table"]//text()').extract()
            result['facts'] = [list(i)
                               for i in zip(fact_table[:: 2],
                                            fact_table[1:: 2])]

            tax_table = sel.xpath(
                '//div[@class="tax-values"]//text()').extract()
            result['taxs'] = [list(i)
                              for i in zip(tax_table[:: 2],
                                           tax_table[1:: 2])]

            listing_detail = sel.xpath(
                '//div[@class="amenities-container"]//text()').extract()
            result['detail'] = listing_detail
            self.detail_statu = False
        else:
            print('---- Detail page not reached')
            logging.warning(
                '---- Detail page url not get, use .search() first to get the detail page')
        return result


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    redfin = Redfin()
    redfin.search()

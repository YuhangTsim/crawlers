"""
logger for twitter crawler
"""
import logging

logger = logging.getLogger('twitterScraper')

formatter = logging.Formatter(
    '-%(asctime)-15s-%(name)s-%(levelname)s: %(message)s')
handler = logging.StreamHandler()
handler2 = logging.FileHandler('./log/scraper.log')
handler.setFormatter(formatter)
handler2.setFormatter(formatter)
logger.addHandler(handler)
logger.addHandler(handler2)
logger.setLevel(logging.INFO)

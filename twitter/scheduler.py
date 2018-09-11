"""
Schedule for crawler
"""
import os
import logging
import random
from apscheduler.schedulers.blocking import BlockingScheduler

logger = logging.getLogger('scheduler')
logger.setLevel(logging.INFO)

formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
sh = logging.StreamHandler()
sh.setFormatter(formatter)
logger.addHandler(sh)

job_defaults = {
    'coalesce': True,
    'max_instances': 3
}

kws = ['healthcare', 'health', 'medical']


def selenium_crawler():
    kw = random.choice(kws)
    os.system(f"python test_cloud.py 40 {kw}")
    logger.info('---- Start')


def scraper():
    kw = random.choice(kws)
    os.system(f"python fetch_tweets.py 50000 {kw}")
    logger.info('---- Start')


if __name__ == '__main__':
    logger.info(f'---- Start in 1 min')
    sched = BlockingScheduler(job_defaults=job_defaults)
    # sched.add_job(selenium_crawler, 'interval', minutes=2)
    sched.add_job(scraper, 'interval', hours=1)
    sched.start()

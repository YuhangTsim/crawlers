"""
Schedule for crawler
"""
import os
import logging
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


def start_crawler():
    os.system("python test_cloud.py 10")
    logger.info('---- Start')


if __name__ == '__main__':
    logger.info('---- Start in 1 min')
    sched = BlockingScheduler(job_defaults=job_defaults)
    sched.add_job(start_crawler, 'interval', minutes=1)
    sched.start()

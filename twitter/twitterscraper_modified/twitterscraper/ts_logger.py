import logging


logger = logging.getLogger('twitterscraper')

formatter = logging.Formatter(
    '-%(asctime)-15s-%(name)s-%(levelname)s: %(message)s')
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)

level = logging.INFO
logger.setLevel(level)

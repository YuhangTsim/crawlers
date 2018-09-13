"""
Speedy, multithreading, Twitter cralwer
Integrated with MySqlclient.

Developed based on twitterscraper.
"""
import json
from json import JSONDecodeError
import random
import datetime as dt
from functools import partial
from multiprocessing.pool import Pool
import time
import requests
import numpy as np
from twitterCrawler.tsLogger import logger
from twitterCrawler.Tweet import Tweet
from utilies.mysqlConnection import mysqlconnector


HEADERS_LIST = [
    'Mozilla/5.0 (Windows; U; Windows NT 6.1; x64; fr; rv:1.9.2.13) Gecko/20101203 Firebird/3.6.13',
    'Mozilla/5.0 (compatible, MSIE 11, Windows NT 6.3; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (Windows; U; Windows NT 6.1; rv:2.2) Gecko/20110201',
    'Opera/9.80 (X11; Linux i686; Ubuntu/14.10) Presto/2.12.388 Version/12.16',
    'Mozilla/5.0 (Windows NT 5.2; RW; rv:7.0a1) Gecko/20091211 SeaMonkey/9.23a1pre']

HEADER = {'User-Agent': random.choice(HEADERS_LIST)}

INIT_URL = 'https://twitter.com/search?f=tweets&vertical=default&q={q}&l={lang}'
RELOAD_URL = 'https://twitter.com/i/search/timeline?f=tweets&vertical=' \
             'default&include_available_features=1&include_entities=1&' \
             'reset_error_state=false&src=typd&max_position={pos}&q={q}&l={lang}'
INIT_URL_USER = 'https://twitter.com/{u}'
RELOAD_URL_USER = 'https://twitter.com/i/profiles/show/{u}/timeline/tweets?' \
                  'include_available_features=1&include_entities=1&' \
                  'max_position={pos}&reset_error_state=false'

dbconfig = {
    'host': '142.93.125.178',
    'user': 'root',
    'pw': 'j3ss3',
    'db': 'twitter'
}


def query_single_page(
        url, html_response=True, retry=10, from_user=False):
    """
    Returns tweets from the given URL.

    :param url: The URL to get the tweets from
    :param html_response: False, if the HTML is embedded in a JSON
    :param retry: Number of retries if something goes wrong.
    :return: The list of tweets, the pos argument for getting the next page.
    """
    try:
        resp = requests.get(url, headers=HEADER)
        if html_response:
            html = resp.text or ''
        else:
            html = ''
            try:
                html = resp.json()['items_html'] or ''
            except JSONDecodeError as e:
                logger.exception(
                    'Failed to parse JSON "{}" while requesting "{}"'.format(
                        e, url))
            except KeyError as e:
                logger.exception(
                    'Failed to parse JSON "{}" while requesting "{}"'.format(
                        e, url))

        tweets = list(Tweet.from_html(html))

        if not tweets:
            return [], None

        if not html_response:
            return tweets, resp.json()['min_position']

        if from_user:
            return tweets, tweets[-1].id
        else:
            return tweets, "TWEET-{}-{}".format(tweets[-1].id, tweets[0].id)

    except requests.exceptions.HTTPError as e:
        logger.exception('HTTPError {} while requesting "{}"'.format(
            e, url))
    except requests.exceptions.ConnectionError as e:
        logger.exception('ConnectionError {} while requesting "{}"'.format(
            e, url))
    except requests.exceptions.Timeout as e:
        logger.exception('TimeOut {} while requesting "{}"'.format(
            e, url))
    except json.decoder.JSONDecodeError as e:
        logger.exception(
            'Failed to parse JSON "{}" while requesting "{}".'.format(e, url))

    if retry > 0:
        logger.info('Retrying... (Attempts left: {})'.format(retry))
        return query_single_page(url, html_response, retry-1)

    logger.error('Giving up.')
    return [], None


def query_tweets_once_generator(query, limit=None, lang='', db=False, kw=''):
    """
    Queries twitter for all the tweets you want! It will load all pages it gets
    from twitter. However, twitter might out of a sudden stop serving new pages,
    in that case, use the `query_tweets` method.

    Note that this function catches the KeyboardInterrupt so it can return
    tweets on incomplete queries if the user decides to abort.

    :param query: Any advanced query you want to do! Compile it at
                    https://twitter.com/search-advanced and just copy the query!
    :param limit: Scraping will be stopped when at least ``limit`` number of
                    items are fetched.
    :param num_tweets: Number of tweets fetched outside this function.
    :param db: Boolean, indicator to use the db connection class, 
               must implement a insert function with one parameter ```tweets```
               allowing multi-record insertion. 
    :return:    If db is not submitted, a list of twitterscraper.Tweet objects. You will get at least
                    ``limit`` number of items;
                If db is submitted, the number of items.
    """
    logger.info(f'-- Quering {query}')
    input_query = query.replace(
        ' ', '%20').replace(
        '#', '%23').replace(
        ':', '%3A')
    mysqldb = mysqlconnector(
        usr=dbconfig['user'],
        pw=dbconfig['pw'],
        db=dbconfig['db'],
        host=dbconfig['host'],
        search_kw=kw)
    pos = None
    num_tweets = 0
    if db:
        tweets = []
    try:
        while True:
            new_tweets, pos = query_single_page(
                INIT_URL.format(q=input_query, lang=lang) if pos is None
                else RELOAD_URL.format(q=query, pos=pos, lang=lang),
                pos is None
            )

            if not db:
                if len(new_tweets) == 0:
                    logger.info('Got {} tweets for {}.'.format(
                        num_tweets, query))
                    return
            else:
                if new_tweets == 0:
                    logger.info('Got {} tweets for {}.'.format(
                        num_tweets, query))
                    return

            if not db:
                for t in new_tweets:
                    yield t, pos
                num_tweets += len(new_tweets)
            else:
                tweets.extend(new_tweets)
                num_tweets += len(new_tweets)

            if num_tweets > 0 and num_tweets % 10000 == 0:
                try:
                    mysqldb.insert(tweets)
                    tweets = []
                except Exception as e:
                    logger.exception('-- Exception : %s', e)

            if limit and num_tweets >= limit:
                logger.info('Limit reached, Got {} tweets for {}.'.format(
                    num_tweets, query))
                break
            time.sleep(1)
        mysqldb.insert(tweets)
        return

    except KeyboardInterrupt:
        logger.info('Program interrupted by user. Returning tweets gathered '
                    'so far...')
        mysqldb.insert(tweets)
    except BaseException:
        logger.exception('An unknown error occurred! Returning tweets '
                         'gathered so far.')
    logger.info('Query done, Got {} tweets for {}.'.format(
        num_tweets, query))


def query_tweets_once(*args, **kwargs):
    res = list(query_tweets_once_generator(*args, **kwargs))
    if res:
        tweets, positions = zip(*res)
        return tweets
    else:
        return []


def query_tweet(
        query, limit=None, beginDate=dt.date(2006, 3, 21),
        endDate=dt.date.today(),
        poolsize=20, lang='', db=False):
    """
    main func to get tweets by advanced search.
    """
    no_days = (endDate - beginDate).days
    if poolsize > no_days:
        poolsize = no_days
    daterange = [
        beginDate + dt.timedelta(days=elem)
        for elem in np.linspace(0, no_days, poolsize+1)]

    if limit:
        limit_per_pool = (limit // poolsize)+1
    else:
        limit_per_pool = None

    queries = [
        '{} since:{} until:{}'.format(query, since, until)
        for since, until in zip(daterange[:-1], daterange[1:])]

    all_tweets = []
    all_tweets_count = 0
    try:
        pool = Pool(poolsize)
        logger.info('-- Queries : {}'.format(queries))
        try:
            for new_tweets in pool.imap_unordered(
                    partial(
                        query_tweets_once,
                        limit=limit_per_pool,
                        lang=lang, db=db, kw=query),
                    queries):
                if not db:
                    all_tweets.extend(new_tweets)
                    logger.info('Got {} tweets ({} new).'.format(
                        len(all_tweets), len(new_tweets)))
                else:
                    all_tweets_count += len(new_tweets)
                    logger.info(
                        f'Got {all_tweets_count} / ({len(new_tweets)} new)')
        except KeyboardInterrupt:
            logger.info('Program interrupted by user. Returning all tweets '
                        'gathered so far.')
    finally:
        pool.close()
        pool.join()

    return all_tweets

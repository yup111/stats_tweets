from __future__ import unicode_literals

import logging
import logging.handlers

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')

import os
import re
import json
import sys
import time
import gzip
import csv
import concurrent.futures
import functools
import multiprocessing as mp
import common
import langid
from preprocess import sanitize
from geocoding.tweet_us_state_geocoder import TweetUSStateGeocoder
import hashlib

tug = TweetUSStateGeocoder()

time_format = '%a %b %d %H:%M:%S +0000 %Y'

def read_folder_worker(tweets_json_gzip):
    tweets = []

    try:
        logger.info('processing: %s'%(tweets_json_gzip))
        with gzip.open(tweets_json_gzip, 'rb') as gzfile:
            for line in gzfile:
                try:
                    line = line.decode("utf-8").strip()


                    ms = re.finditer(r'((^|\})\d+\.\d+\t|.*?)(\{.*\})', line)

                    for m in ms:
                        json_string = m.group(3)
                        tweet = json.loads(json_string)

                        geolocation = tweet['user']['location']

                        if 'place' in tweet and tweet['place']:

                           if (tweet['place']['country_code'] != 'US'):
                               continue
                           else:
                               geolocation = tweet['place']['full_name']


                        us_state = tug.get_state(geolocation)

                        if (not us_state):
                           us_state = "Null"

                        tweets.append({
                            'id': tweet['id'],
                            'text':common.clean_tweet_text(tweet['text']),
                            'clean_text':sanitize(tweet['text']),
                            'place': tweet['place'] if 'place' in tweet else '',
                            'user_location': tweet['user']['location'],
                            'created_at': tweet['created_at'],
                            'username': tweet['user']['name'],
                            'user_id': tweet['user']['id'],
                            'month': common.time_str_to_month(tweet['created_at']),
                            "filename": tweets_json_gzip,
                            'week': common.time_str_to_week(tweet['created_at']),
                            'state': us_state
                            })

                except Exception as exc:
                    logger.warn('in %s: ignore: %s'%(tweets_json_gzip, exc))

    except Exception as exc:
        logger.warn('in %s: ignore: %s'%(tweets_json_gzip, exc))
    return tweets


def read_folder_callback(future, all_tweets = {}):
    tweets = future.result()
    for tweet in tweets:
        if (tweet['month'] not in all_tweets):
            all_tweets[tweet['month']] = []

        all_tweets[tweet['month']].append(tweet)

def stats_by_month(all_tweets, outputfilename):
    stats = {}

    tweetids = set()
    tweetuserids = set()
    tweetusergeoids = set()

    for month, tweets in all_tweets.items():
        if (month not in stats):
            stats[month] = {
            'month': month,
            'total': len(tweets),
            'after_filter': 0,
            'total geo tweets': 0,
            'total user': 0,
            'total geo user': 0
            }
        else:
            stats[month]['total'] += len(tweets)

        for tweet in tweets:
            if tweet['id'] not in tweetids:
                stats[month]['after_filter'] += 1
                tweetids.add(tweet['id'])

            if tweet['state'] != "Null":
                stats[month]['total geo tweets'] += 1

                if tweet['user_id'] not in tweetusergeoids:
                    stats[month]['total geo user'] += 1
                    tweetusergeoids.add(tweet['user_id'])

            if tweet['user_id'] not in tweetuserids:
                stats[month]['total user'] += 1
                tweetuserids.add(tweet['user_id'])

    return stats

def write_csv(dicts, filename, headnames):
    with open(filename, 'a+', newline='', encoding='utf-8') as csv_f:
        writer = csv.DictWriter(csv_f, fieldnames=headnames, delimiter=',', quoting=csv.QUOTE_ALL)
        writer.writeheader()

        for month, stat in stats.items():
            writer.writerow(stat)

def read_folder(input_folder):
        start_time = time.time()
        futures_ = []
        all_tweets = {}
        max_workers = mp.cpu_count()
        with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
            for root, dirs, files in os.walk(os.path.abspath(input_folder)):
                for f in files:
                    logger.info('I fetch the file %s' %f)

                    if (f.endswith('.json')):
                        continue
                    future_ = executor.submit(read_folder_worker, os.path.join(root, f))
                    future_.add_done_callback(functools.partial(read_folder_callback, all_tweets=all_tweets))
                    futures_.append(future_)
            else:
                concurrent.futures.wait(futures_)
                executor.shutdown()

                logger.info('processed in [%.2fs]' % ((time.time() - start_time)))

                return all_tweets


if __name__ == "__main__":

    handler = logging.handlers.RotatingFileHandler(
        'stats_tweets.log', maxBytes=10 * 1024 * 1024, backupCount=10)
    logger.addHandler(handler)
    logger.info(sys.version)

    all_tweets = read_folder('./')
    write_csv(all_tweets, "give a name", [
                                        'id',
                                        'text',
                                        'clean_text',
                                        'place',
                                        'user_location',
                                        'created_at',
                                        'username',
                                        'user_id',
                                        'month',
                                        'week',
                                        'state'
                                        ])

    stats = stats_by_month(all_tweets)
    write_csv(stats, "give a name",  [
                                    'month',
                                    'total',
                                    'after_filter',
                                    'total geo tweets',
                                    'total user',
                                    'total geo user'
                                    ])
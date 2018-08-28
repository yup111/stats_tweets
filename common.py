#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')

import re, os, csv, json
from ftfy import fix_text

twitter_username_p = re.compile(r'(?<=^|(?<=[^a-zA-Z0-9-_]))(@[A-Za-z]+[A-Za-z0-9_]+)')
url_p = re.compile(r'http[s]?:.*?(\s+|$)')
hashtag_p = re.compile(r'#\w+')
email_p = re.compile(r'(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)')
nonvalid_characters_p = re.compile("[^a-zA-Z0-9#\*\-_\s]")
non_ascii_p = re.compile(r'[^\x00-\x7F]+')
hashtag_sign_p = re.compile(r'#')

def cleanhtml(raw_html):
  cleanr = re.compile('<.*?>')
  cleantext = re.sub(cleanr, '', raw_html)
  return cleantext
  
def remove_hashtag_sign(text):
    text = re.sub(hashtag_sign_p, '', text)
    return text
    
def clean_tweet_text(text):
    text = fix_text(text.replace('\r\n',' ').replace('\n',' ').replace('\r',' '))

    text = re.sub(non_ascii_p, '', text)

    return text.strip()

def remove_url(text):
    text = re.sub(url_p, '', text)

    return text

def remove_username(text):
    text = re.sub(twitter_username_p, '', text)

    return text

def alpha_and_number_only(text):

    text = re.sub(nonvalid_characters_p, ' ', text)
    #text = text.replace('-', ' ')

    return text

def sanitize_text(text):

    return alpha_and_number_only(remove_username(remove_url(clean_tweet_text(text))))

def has_url(text):

    m = re.findall(url_p, text)

    return 1 if m else 0

# text = 'RT @JHSty: @BillSchulz Try this: Smokey Robinson & The Miracles-The Tears Of A Clown http://bit.ly/EFjjb  Hang in...Great Song!!!'
# logger.info(has_url(text))

# quit()

def has_username(text):

    m = re.findall(twitter_username_p, text)

    return 1 if m else 0

parse_time_format = '%a %b %d %H:%M:%S +0000 %Y'
day_output_date_format = '%Y%m%d_%a'
month_output_date_format = '%Y%m'
week_output_date_format = '%Y_%U'
import time
from datetime import datetime, timedelta

def time_str_to_day(time_str):

    t = time.strptime(time_str, parse_time_format)

    return time.strftime(day_output_date_format, t)

def time_str_to_month(time_str):

    t = time.strptime(time_str, parse_time_format)

    return time.strftime(month_output_date_format, t)

def time_str_to_week(time_str, parse_time_format=parse_time_format):

    t = time.strptime(time_str, parse_time_format)

    return time.strftime(week_output_date_format, t)

# week starts on Monday
# def time_str_to_weekday(time_str, parse_time_format=parse_time_format):
#     dt = datetime.fromtimestamp(time.mktime(time.strptime(time_str, parse_time_format)))

#     week = []
#     start = dt - timedelta(days = dt.weekday())

#     for i in range(7):
#         current = start + timedelta(days = i)
#         week.append(current.strftime(day_output_date_format))
#     #end = start + timedelta(days = 6)
#     return week

def time_str_to_weekday(week_start_time_str, parse_time_format=parse_time_format):
    start = datetime.fromtimestamp(time.mktime(time.strptime(week_start_time_str, parse_time_format)))

    week = []
    for i in range(7):
        current = start + timedelta(days = i)
        week.append(current.strftime(day_output_date_format))
    #end = start + timedelta(days = 6)
    return week

def get_fieldnames(csv_filename):
    fieldnames = []
    with open(csv_filename, 'r', newline='', encoding='utf-8') as rf:
        reader = csv.reader(rf)

        header = next(reader)

        for field in header:
            fieldnames.append(field)

    return fieldnames

def find_matched_terms(text, terms):
    text = remove_url(remove_username(clean_tweet_text(text))).lower()

    matched_terms = {}
    cnt = 0
    for main_concept in terms:
        matched_terms[main_concept] = []
        for term in terms[main_concept]:
            term = term.strip()
            original_term = term
            if ('*' in term):
                term = term.replace('*', '')
                term = re.escape(term)
                term = '%s.*?'%(term)
            else:
                term = re.escape(term)

            #m = re.findall(r'(?:^|\*|\s)(%s)(?=\*|\s|$)'%(re.escape(term)), text)

            pattern = re.compile(r'(?:^|\*|\s)(%s)(?=\*|\s|$)'%term)

            m = re.findall(pattern, text)

            if (m):
                matched_terms[main_concept].extend(original_term)
                cnt += 1

    for main_concept in terms:
        matched_terms[main_concept] = list(set(matched_terms[main_concept]))

    return cnt, matched_terms

def print_dict(hashmap):
    for key, value in hashmap.items():
        logger.info('%s: %d'%(key,value))

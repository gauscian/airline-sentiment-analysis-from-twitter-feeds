import threading

import pandas as pd
import numpy as np
import json
import io
import datetime as dt
import string
import unicodedata
import nltk
from nltk.tokenize.toktok import ToktokTokenizer
tokenizer = ToktokTokenizer()
stopword_list = nltk.corpus.stopwords.words('english')
import spacy
import en_core_web_sm
nlp = en_core_web_sm.load()
import re
from bs4 import BeautifulSoup
from gensim import corpora, models, similarities
import os
import time
# =====================================================================
import dcl_twitter_doctor


# should get sublist of tweets
def worker(name, sublist):
    # print('mer naam hai = ',name)
    for i, each in enumerate(sublist):
        sublist[i] = dcl_twitter_doctor.strip_links(each)
        sublist[i] = dcl_twitter_doctor.strip_mentions(sublist[i])
        sublist[i] = dcl_twitter_doctor.strip_hashtags(sublist[i])
        sublist[i] = sublist[i].replace('RT', '')
        sublist[i] = dcl_twitter_doctor.expandContractions(sublist[i])
        sublist[i] = dcl_twitter_doctor.remove_special_characters(sublist[i])
        # sublist[i] = dcl_twitter_doctor.remove_stopwords(sublist[i])
        # sublist[i] = dcl_twitter_doctor.simple_stemmer(sublist[i])

        # here we can lemmatize_text

    file = open("data/"+name+".txt", 'w', encoding="utf-8")
    for text in sublist:
        file.write(text+"\n")

    file.close()

    # print("completed = ",name)



def clean_tweets(n_threads = 7):

    start = time.time()
    # read the csv of tweets
    df = pd.read_csv('Tweets.csv')
    df = df.text.dropna()
    df = df.reset_index(drop=True)

    # create the output folder
    if not os.path.exists("data/"):
        os.makedirs("data/")


    # crating a tweet list instead of a dictionary
    tweet_dictionary = []
    for line in df:
        tweet_dictionary.append(line.lower())

    # prepare for slicing
    total_tweets = len(tweet_dictionary)

    n_threads += 1

    thread_list = []
    n_batch = total_tweets // n_threads
    # creating the threads
    cut = 0
    for i in range(0, n_threads):
        # print(cut, cut+n_batch)
        thread_list.append(threading.Thread(name = str(i), target = worker,
                            args = (str(i), tweet_dictionary[cut:(cut+n_batch)])))

        cut += n_batch
        thread_list[i].daemon = True

    # starting the threads
    for i in range(n_threads):
        thread_list[i].start()

    # joining the threads
    for i in range(n_threads):
        thread_list[i].join()


    # compile everything into one file
    all_data_files = os.listdir("data/")

    file = open("master.txt", 'w', encoding="utf-8")



    res_tweets = []
    for each_file in all_data_files:
        f_ = open("data/"+each_file, 'r',encoding="utf-8")
        lines = f_.readlines()
        for line in lines:
            file.write(line)
            res_tweets.append(line.rstrip())

        f_.close()

    file.close()

    print("completed in = ",time.time() - start)
    return res_tweets

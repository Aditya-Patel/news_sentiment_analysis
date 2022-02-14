import math
from nltk.corpus import sentiwordnet as swn

magnitude_dict = {1:'mildly', 2:'moderately', 3:'strongly', -1:'very strongly'}

def get_senti_score(word):
    swn_tup = swn.senti_synsets(word)
    try:
        swn_list = list(swn_tup)[0]
        return (swn_list.pos_score() - swn_list.neg_score())
    except Exception:
        return 0

def text_cleansing(text=""):
    punc = '''!()[]{};:"\,<>./?@#$%^&*_~'''
    for mark in punc:
        if mark in text:
            text = text.replace(mark, "")
    text_list = text.split(' ')
    return text_list

def get_value(sentiment):
    if sentiment == 0:
        return 0, "neutral"
    elif sentiment > 0:
        return 1, " positive"
    else:
        return -1, " negative"

def get_magnitude(sentiment):
    if abs(sentiment) == 0:
        return 0, " "
    rnd_abs_sent = round(abs(sentiment))
    if rnd_abs_sent in magnitude_dict.keys():
        return rnd_abs_sent, magnitude_dict.get(rnd_abs_sent)
    else:
        return 4, magnitude_dict.get(-1)

def get_sentiment_vector(sentiment):
    binned_score, magnitude = get_magnitude(sentiment)
    multiplier, value = get_value(sentiment)

    return (multiplier * binned_score), f"{magnitude}{value}"

def get_text_sentiment(text):
    text_list = text_cleansing(text)
    net_sentiment = 0
    for word in text_list:
        word_score = get_senti_score(word)
        net_sentiment += word_score
    return get_sentiment_vector(net_sentiment)

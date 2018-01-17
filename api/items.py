from pymongo import MongoClient
import json
import os
from datetime import datetime
from bson import json_util
import pandas as pd


# Config
BASE_PATH = os.path.dirname(__file__)
DB_CONFG_PATH_QALI = os.path.normpath(os.path.join(BASE_PATH, '../', 'config/dbconfig.json'))


# Connect to the database
cfg = json.load(open(DB_CONFG_PATH_QALI))
#db = 'mongodb://{user}:{pass}@{host}:{port}'.format(**cfg)
db = 'mongodb://{host}:{port}'.format(**cfg)


with MongoClient(db) as client:
    tweets = client[cfg['database']][cfg['collection'][0]]
    scores = client[cfg['database']][cfg['collection'][1]]


# Methods
def getCoin(coinname) -> object:

    coin_info = scores.find_one({'currency': coinname})

    return json.dumps(coin_info, default=json_util.default)


def getCoinScores(coinname) -> object:

    coin_info = scores.find_one({'currency': coinname})

    coin_scores = pd.DataFrame(columns=['date', 'price_eur', 'avg_sent'])

    for score in coin_info['scores']:
        coin_scores.append([score['date'], score['price_eur'], score['avg_sent']])

    return coin_scores

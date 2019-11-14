from csv import DictReader
from os import listdir, getcwd, path
from pymongo import MongoClient
from yaml import load, BaseLoader
from pprint import pprint


def make_collections(config, db):
    collection_map = {}
    return collection_map


def process_hashtags_in_tweets(row):
    hashtag_map = {}
    return hashtag_map


def insert_into_default_collection(collection, list_to_insert):
    pass


def get_info_and_insert_into_collection(filepath, config, db):
    def_list_to_insert = []
    hashtag_list_to_insert = []
    collection_map = make_collections(config, db)
    default_collection = collection_map.get('default').get('name', 'tweets')
    hashtag_collection = collection_map.get('hashtags').get('name', 'hashtags')
    with open(filepath, newline='', encoding='utf-8') as csvfile:
        r = DictReader(csvfile, fieldnames=None, delimiter=',', quotechar='"')
        for row in r:
            def_list_to_insert.append(dict(row))
            if len(def_list_to_insert) > 100:
                insert_into_default_collection(default_collection, def_list_to_insert)
                def_list_to_insert = []


def process_files(config):
    client = MongoClient(host=config.get('host', 'localhost'), port=int(config.get('port', 27017)), appname='test_scripts')
    db = client.get_database(name=config.get('db', 'tweet_db'))
    tweets_path = getcwd() + path.sep + config.get('tweet_folder', 'tweets')
    for file_name in listdir(tweets_path):
        file_path = tweets_path + path.sep + file_name
        get_info_and_insert_into_collection(file_path, config, db)


def get_config(config_file_path='config.yaml'):
    with open(config_file_path, 'r') as config_file:
        return load(config_file.read(), Loader=BaseLoader)


if __name__ == '__main__':
    pprint(get_config())

from csv import DictReader
from os import listdir, getcwd, path
from pymongo import MongoClient, errors
from yaml import load, BaseLoader
from pprint import pprint

DEBUG = False
INSERT_COUNT = 0

def get_collections(config, db):
    collection_map = {}
    for col in config.get('collections', []):
        collection_name = col.get('name')
        try:
            mongo_collection = db.create_collection(
                name=collection_name
            )
            for index in col.get('indices', []):
                mongo_collection.create_index(index)
        except errors.CollectionInvalid as e:
            print(f"collection {collection_name} already exists")
            mongo_collection = db.get_collection(collection_name)
        collection_map[collection_name] = mongo_collection
    return collection_map


def process_hashtags_in_tweets(row, cache_tags):
    row_dict = dict(row)
    tweet = row_dict.get('content', '')
    for word in tweet.split():
        if word.startswith("#"):
            tag_without_hash = word.strip('#')
            cache_tags[tag_without_hash] = cache_tags[tag_without_hash] + 1 if (cache_tags.get(tag_without_hash)) else 1


def insert_dict_into_collection(collection, dict_to_insert):
    for key, count in dict_to_insert.items():
        collection.insert_one({
            "hashtag": key,
            "count": count
        })
    if DEBUG:
        print(f"inserted hashtag dict into: {collection.name}")
        print(f"inserted count: {len(dict_to_insert)}")


def insert_list_into_collection(collection, list_to_insert):
    collection.insert_many(list_to_insert)
    if DEBUG:
        global INSERT_COUNT
        INSERT_COUNT += len(list_to_insert)
        print(f"inserting list into: {collection.name}")
        print(f"first list item: {list_to_insert[0]}")
        print(f"inserted count: {INSERT_COUNT}")



def get_info_and_insert_into_collection(filepath, collection_map, cache_tags):
    tweet_list_to_insert = []
    default_collection = collection_map.get('tweets')
    with open(filepath, newline='', encoding='utf-8') as csvfile:
        r = DictReader(csvfile, fieldnames=None, delimiter=',', quotechar='"')
        for row in r:
            tweet_list_to_insert.append(dict(row))
            process_hashtags_in_tweets(row=row, cache_tags=cache_tags)
            if len(tweet_list_to_insert) > 100:
                insert_list_into_collection(default_collection, tweet_list_to_insert)
                tweet_list_to_insert = []


def get_db_from_config(config):
    client = MongoClient(host=config.get('host', 'localhost'), port=int(config.get('port', 27017)),
                         appname='test_scripts')
    db = client.get_database(name=config.get('db', 'tweet_db'))
    return db


def process_files(config):
    db = get_db_from_config(config)
    collection_map = get_collections(config, db)
    tweets_path = getcwd() + path.sep + config.get('tweet_folder', 'tweets')
    cache_tags = {}
    for file_name in listdir(tweets_path):
        file_path = tweets_path + path.sep + file_name
        get_info_and_insert_into_collection(file_path, collection_map, cache_tags)
    hashtag_collection = collection_map.get('hashtags')
    insert_dict_into_collection(hashtag_collection, cache_tags)


def get_config(config_file_path='config.yaml'):
    with open(config_file_path, 'r') as config_file:
        config = load(config_file.read(), Loader=BaseLoader)
        global DEBUG
        DEBUG = config.get("debug", False)
        return config


def clear_database(config):
    db = get_db_from_config(config)
    collection_map = get_collections(config, db)
    for col in collection_map.values():
        col.delete_many({})  # DELETES ALL DOCS!
        col.drop_indexes()

if __name__ == '__main__':
    process_files(get_config())

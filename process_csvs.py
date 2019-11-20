from csv import DictReader
from os import listdir, getcwd, path
from pymongo import MongoClient, errors
from yaml import load, BaseLoader

INSERT_COUNT = 0


def get_collections(config, db, debug=False):
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
            if debug:
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


def insert_dict_into_collection(collection, dict_to_insert, debug=False):
    insert_list = []
    for key, count in dict_to_insert.items():
        insert_list.append({
            "hashtag": key,
            "count": count
        })
        if len(insert_list) > 10000:
            insert_list_into_collection(collection, insert_list, debug)
            insert_list = []
        insert_list_into_collection(collection, insert_list, debug)


def insert_list_into_collection(collection, list_to_insert, debug=False):
    res = collection.insert_many(list_to_insert)
    if debug:
        print(f"inserting list into: {collection.name}")
        print(f"inserted {res.inserted_count} records")


def get_db_from_config(config):
    client = MongoClient(host=config.get('host', 'localhost'), port=int(config.get('port', 27017)),
                         appname='test_scripts')
    db = client.get_database(name=config.get('db', 'tweet_db'))
    return db


def process_file(file_path, tweet_collection, hashtag_collection, cache_tags, debug=False):
    raw_tweet_list = []
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        r = DictReader(csvfile, fieldnames=None, delimiter=',', quotechar='"')
        for row in r:
            if tweet_collection:
                raw_tweet_list.append(row)
                if len(raw_tweet_list) > 10000:
                    insert_list_into_collection(tweet_collection, raw_tweet_list, debug)
                    raw_tweet_list = []
            if hashtag_collection:
                process_hashtags_in_tweets(row=row, cache_tags=cache_tags)
        if tweet_collection and len(raw_tweet_list) > 0:
            insert_list_into_collection(tweet_collection, raw_tweet_list, debug)


def check_for_collections(collections_map):
    hashtag_collection = collections_map.get("hashtags", None)
    tweet_collection = collections_map.get("tweets", None)
    if not tweet_collection and not hashtag_collection:
        raise Exception("no collections were found to add to!")
    return hashtag_collection, tweet_collection


def process_files(config, collections_map, debug=False):
    hashtag_collection, tweet_collection = check_for_collections(collections_map)
    tweets_path = getcwd() + path.sep + config.get('tweet_folder', 'tweets')
    cache_tags = {}
    for file_name in listdir(tweets_path):
        file_path = tweets_path + path.sep + file_name
        process_file(file_path, tweet_collection, hashtag_collection, cache_tags, debug)
    if cache_tags and hashtag_collection:
        insert_dict_into_collection(collections_map.get('hashtags'), cache_tags, debug)


def get_config(config_file_path='config.yaml'):
    with open(config_file_path, 'r') as config_file:
        return load(config_file.read(), Loader=BaseLoader)


def clear_database(config):
    db = get_db_from_config(config)
    collection_map = get_collections(config, db)
    for col in collection_map.values():
        col.delete_many({})  # DELETES ALL DOCS!
        col.drop_indexes()

from csv import DictReader
from os import listdir, getcwd, path
from pymongo import MongoClient
MONGO_HOST = 'localhost'
MONGO_PORT = 27017

client = MongoClient(host=MONGO_HOST, port=MONGO_PORT, appname='test_scripts')


def create_database():
    mongo_db = client.tweet_db
    tweet_collection = mongo_db.tweet_collection



def get_info_and_insert_into_collection(filepath):
    list_to_insert = []
    with open(filepath, newline='', encoding='utf-8') as csvfile:
        r = DictReader(csvfile, fieldnames=None, delimiter=',', quotechar='"')
        for row in r:
            list_to_insert.append(dict(row))
            if len(list_to_insert) > 100:
                result = tweet_collection.insert_many(list_to_insert)
                print(result.inserted_ids)
                list_to_insert = []


def process_files():
    tweets_path = getcwd() + path.sep + "tweets"
    for file_name in listdir(tweets_path):
        file_path = tweets_path + path.sep + file_name
        get_info_and_insert_into_collection(file_path)


if __name__ == '__main__':
    process_files()
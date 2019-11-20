import argparse
import process_csvs as pc


def get_desc():
    return """Use this tool to populate data in mongo.  Without any arguments, this will read configuration
    data from config.yaml and add it to a mongodb on localhost:27017.  It will check if the collections
    it wants to create are already populated, and if so it will quit (unless the -f flag is supplied)."""


def populate_data(config, force=False, debug=False):
    db = pc.get_db_from_config(config)
    collection_candidates = pc.get_collections(config, db, debug)
    collections = collection_candidates.copy()
    if not force:
        for name, collection in collection_candidates.items():
            if collection.count_documents({}) > 0:
                print(f"collection {collection.name} not empty, removing!")
                collections.pop(name)
    try:
        pc.process_files(config, collections, debug)
    except Exception as e:
        print(e)


def destroy_data(config, debug=False):
    if debug:
        print("erasing docs from collections")
    try:
        pc.clear_database(config)
        if debug:
            print("collections cleared")
    except Exception as e:
        print(e.with_traceback())


def process_args(args):
    config_fp = args.config if args.config else "config.yaml"
    config = pc.get_config(config_fp)
    debug = True if args.verbose or config.get('debug', '') else False
    if args.delete:
        destroy_data(config, debug)
    else:
        populate_data(config, args.force, debug)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=get_desc())
    parser.add_argument("-c", "--config", type=str, help="path to the config yaml (default: ./config.yaml)")
    parser.add_argument("-v", "--verbose", action="store_true", help="print verbose output")
    parser.add_argument("-d", "--delete", action="store_true", help="delete all docs and indices from all collections " +
                                                                    "in config file.  Quit afterwards")
    parser.add_argument("-f", "--force", action="store_true", help="skip population checks and force-populate data")
    args = parser.parse_args()
    process_args(args)

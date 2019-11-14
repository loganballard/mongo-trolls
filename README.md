# Loading the tweets into a mongo database

to obtain the tweets:

`$ git clone https://github.com/fivethirtyeight/russian-troll-tweets.git`
- put them into the tweets folder
- (probably do this in a virtual environment)
    - `$ python3 -m virtualenv venv`
    - `$ source venv/bin/activate`
- `pip install -r requirements.txt`
- create a local mongo running instance 
    - install the `mongo` / `mongos` / `mongod` binaries
    - `$ mongod`
- `$ python process_csvs.py`
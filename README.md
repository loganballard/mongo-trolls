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
- Edit the `config.yaml` file with your specifications.  By default the specifications should be correct for the default mongodb setup
- Use the command line driver to insert all these records into your local mongodb. 
- `$ python driver.py -h`
    - outputs the help command
- `$ python driver.py`
    - without any arguments, the driver loads all collections specified in the `config.yaml` file in the root directory and loads those collections with tweets from `./tweets`
- `$ python driver.py `
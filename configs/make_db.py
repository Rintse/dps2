from pymongo import MongoClient
from pymongo import DESCENDING
from sys import argv
from urllib.parse import quote_plus

# Mongo server credentials
MONGO_USER = "storm"
MONGO_PASSW = "test"

# Arguments
if len(argv) < 2:
    print("Wrong params")
    exit(1)
mongoHost = argv[1]

# Initiate client
client = MongoClient( \
    'mongodb://%s:%s@%s' % ( \
        quote_plus(MONGO_USER),
        quote_plus(MONGO_PASSW),
	quote_plus(mongoHost + ":" + "27017")
    )
)

# The results are stored in:
# table: "results", collection "aggregation"
results = client['results']['aggregation']

# List with all the states 
states = open("DPS2/data/states.dat", "r").read().splitlines()

for s in states: # Make an entry with all votes set to 0
    results.insert_one({"state" : s, "Rvotes" : 0, "Dvotes" : 0})

results.create_index([("state", DESCENDING)])

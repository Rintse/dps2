from pymongo import MongoClient
from sys import argv
from urllib.parse import quote_plus

# Mongo server credentials
MONGO_USER = "storm"
MONGO_PASSW = "test"

# Arguments
if len(argv) < 3:
    print("Wrong params")
    exit(1)
mongoHost = argv[1]
mongoPort = argv[2]

# Initiate client
client = MongoClient( \
    'mongodb://%s:%s@%s' % ( \
        quote_plus(MONGO_USER),
        quote_plus(MONGO_PASSW),
	quote_plus(mongoHost + ":" + mongoPort)
    )
)

# The results are stored in:
# table: "results", collection "aggregation"
results = client['results']['aggregation']

# List with all the counties 
counties = open("counties.dat", "r").read().splitlines()

for c in counties: # Make an entry with all votes set to 0
    results.insert_one({"county" : c, "Rvotes" : 0, "Dvotes" : 0})

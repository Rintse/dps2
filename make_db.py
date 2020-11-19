from pymongo import MongoClient
from sys import argv

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
    'mongodb://%s:%s@' \
    + mongoHost + ":" + mongoPort \
    % (MONGO_USER, MONGO_PASSW)
)

# The results are stored in:
# table: "results", collection "aggregation"
results = client['results']['aggregation']

counties = open("../counties.dat", "r").read().splitlines()

for c in counties:
    results.insert_one({"county" : c, "Rvotes" : 0, "Dvotes" : 0})

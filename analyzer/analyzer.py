from pymongo import MongoClient
from counties import get_county_to_state_map
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

county_map = get_county_to_state_map()

# Initiate client
client = MongoClient( \
    'mongodb://%s:%s@' \
    + mongoHost + ":" + mongoPort \
    % (MONGO_USER, MONGO_PASSW)
)

# The results are stored in:
# table: "results", collection "aggregation"
results = client['results']['aggregation']

# The timestamp of the last processed result
# Only take stuff beyond this point
last_processed = 0

run = True
while run:
    new_results = results.find({ "time" : {"$gt": last_processed} })

from pymongo import MongoClient
import plotly.express as px
import json
from sys import argv
from numpy.random import randint

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
#  client = MongoClient( \
    #  'mongodb://%s:%s@' \
    #  + mongoHost + ":" + mongoPort \
    #  % (MONGO_USER, MONGO_PASSW)
#  )

# The results are stored in:
# table: "results", collection "aggregation"
# results = client['results']['aggregation']

county_list = open("../counties.dat").read().splitlines()
counties = json.load(open("geojson_counties.json"))

run = True
while run:
    # Retreive aggregation result data
    #data = results.find()
    data = [
        {
            "county" : i , 
            "Rvotes" : randint(100, 1000*1000), 
            "Dvotes" : randint(100, 1000*1000)
        }
        for i in county_list
    ]
    # Add field with winner
    for d in data:
        d["winner"] = "Republican" if d["Rvotes"] > d["Dvotes"] else "Democrat"

    fig = px.choropleth(
        data,
        geojson=counties,
        locations='county', 
        color='winner',
        scope="usa",
        hover_data=["Rvotes", "Dvotes"],
        color_discrete_map={ "Republican": "#bd3428", "Democrat": "#346fed" }
    )
    fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
    fig.show()


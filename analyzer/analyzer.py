from pymongo import MongoClient
import plotly.express as px
import json
from sys import argv
from numpy.random import randint
from us import states
import time

# Mongo server credentials
MONGO_USER = "storm"
MONGO_PASSW = "test"

# Arguments
if len(argv) < 3:
    print("Wrong params")
    exit(1)
mongoHost = argv[1]
mongoPort = argv[2]

def random_test_data():
    return [
        {
            "county" : i , 
            "Rvotes" : randint(100, 1000*1000), 
            "Dvotes" : randint(100, 1000*1000)
        }
        for i in county_list
    ]

import csv


# Initiate client
client = MongoClient( \
    'mongodb://%s:%s@' \
    + mongoHost + ":" + mongoPort \
    % (MONGO_USER, MONGO_PASSW)
)

# The results are stored in:
# table: "results", collection "aggregation"
results = client['results']['aggregation']

county_list = open("DPS/data/counties.dat").read().splitlines()
counties_json = json.load(open("DPS/data/geojson_counties.json"))
state_map = states.mapping('fips', 'name')

state_data = { 
    str(state) : { "Dvotes": 0, "Rvotes": 0, "winner" : "" } 
    for state in states.STATES
}
state_data["total"] = { "Dvotes": 0, "Rvotes": 0, "winner" : "" }

def determine_winner(elem):
    elem["winner"] = "Republican" if elem["Rvotes"] > elem["Dvotes"] else "Democrat"

def tally_votes(county):
    state_name = state_map[ county["county"][0:2] ]
    rep_votes = county["Rvotes"]
    dem_votes = county["Dvotes"]

    state_data[state_name]["Rvotes"] += rep_votes
    state_data[state_name]["Dvotes"] += dem_votes
    state_data["total"]["Rvotes"] += rep_votes
    state_data["total"]["Dvotes"] += dem_votes

    determine_winner(county)

def make_figure(data):
    fig = px.choropleth(
        data,
        geojson=counties_json,
        locations='county', 
        color='winner',
        scope="usa",
        hover_data=["Rvotes", "Dvotes"],
        color_discrete_map={ "Republican": "#bd3428", "Democrat": "#346fed" }
    )
    fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
    # fig.png


run = True
while run:
    # Retreive aggregation result data
    data = results.find() 
    
    # Calculate various aggregates over the counties
    for county in data:
        tally_votes(county)        
   
    for state in state_data:
        determine_winner(state)
    
    # Figure of counties coloured by winner
    make_figure(data)

    print("Total votes cast: ", state_data["total"]["Rvotes"] + state_data["total"]["Dvotes"])

    time.sleep(8)

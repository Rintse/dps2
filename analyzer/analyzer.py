#!/cm/shared/package/python/3.5.2/bin/python3.5 

from pymongo import MongoClient
import plotly.express as px
import plotly
import json
from sys import argv
from numpy.random import randint
import us
import time
from urllib.parse import quote_plus

# Mongo server credentials
MONGO_USER = "storm"
MONGO_PASSW = "test"

# Arguments
if len(argv) < 2:
    print("Wrong params")
    exit(1)
mongoHost = argv[1]

state_list = open("DPS2/data/states.dat").read().splitlines()

def random_test_data():
    global state_list

    return [
        {   "state" : i , 
            "Rvotes" : randint(100, 1000*1000), 
            "Dvotes" : randint(100, 1000*1000)  }
        for i in state_list
    ]


def determine_winner(elem):
    elem["winner"] = "Republican" if elem["Rvotes"] > elem["Dvotes"] else "Democrat"

def make_figure(data):
    fig = px.choropleth(
        data,
        locations='state', 
        color='winner',
        scope="usa",
        locationmode="USA-states",
        hover_data=["Rvotes", "Dvotes"],
        color_discrete_map={ "Republican": "#bd3428", "Democrat": "#346fed" }
    )
    fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
    # Save to the webserver folder
    plotly.offline.plot(fig, auto_open=False, filename="DPS2/webserver/index.html")


while True:
    try:
        #  Initiate client
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
        run = True
        while run:
            # Retreive aggregation result data
            data = list(results.find())
            
            for state in data:
                determine_winner(state)

            # Calculate total votes
            total_votes = { "Democrat" : 0, "Republican" : 0, "Total" : 0 }
            for state in data:
                total_votes["Democrat"] += state["Dvotes"]
                total_votes["Republican"] += state["Rvotes"]
                total_votes["Total"] += state["Dvotes"] + state["Rvotes"]

            # Figure of states coloured by winner
            make_figure(data)

            print("Total votes cast: ", total_votes["Total"])

            time.sleep(4)

    except pymongo.errors.ConnectionFailure:
        print("Connection to mongo lost, retrying")

#!/bin/python3
from numpy import quantile
from numpy import mean
from os import listdir, chdir
from pprint import pprint
from os.path import isfile, join

QUANTILES = [0.9, 0.95, 0.99]
FIELDS = ["throughput", "avg_latency", "min_latency", "max_latency", "quantiles(90,95,99)"]

files = dict()

chdir("throughputs")

files[1] = ["1nodes/"+f for f in listdir("1nodes") if ".res" in f]
files[2] = ["2nodes/"+f for f in listdir("2nodes") if ".res" in f]
files[4] = ["4nodes/"+f for f in listdir("4nodes") if ".res" in f]
files[8] = ["8nodes/"+f for f in listdir("8nodes") if ".res" in f]

for num_nodes, results in files.items():
    print(num_nodes, "nodes:")
    print(", ".join(FIELDS))
    
    for i in sorted(results, reverse=True):
        latencies = [ float(l.split(",")[1]) for l in open(i).read().splitlines()[1:] ]
        
        throughput = i.split("_")[0].split("/")[1]
        avg_lat = mean(latencies)
        min_lat = min(latencies)
        max_lat = max(latencies)
        quantiles = quantile(latencies, QUANTILES)
        
        print(throughput, avg_lat, min_lat, max_lat, quantiles)
    print()

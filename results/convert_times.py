#!/bin/python3
from sys import argv
import pprint
import matplotlib.pyplot as plt

file = open(argv[1])
lines = file.read().splitlines()[1:]
data = [ l.split(',') for l in lines ]

data.sort(key=lambda x: float(x[0]))

times = []
latencies = []

start = -1.0
for line in data:
    if start == -1.0:
        start = float(line[0])
    
    time = float(line[0]) - start
    
    times.append(time)
    latencies.append(float(line[1]))

    #print(time, " ", line[1])

plt.scatter(times, latencies)
plt.ylabel('Latency (s)')
plt.xlabel('Runtime (s)')
plt.show()

#!/bin/python3
from sys import argv
import pprint
import math
import matplotlib.pyplot as plt

WARMUP_FRAC = 0.10

file = open(argv[1])
lines = file.read().splitlines()[1:]
data = [ l.split(',') for l in lines ]

data.sort(key=lambda x: float(x[0]))

entries = []
min_time = math.inf
max_time = -math.inf

start = -1.0
for line in data:
    if start == -1.0:
        start = float(line[0])
    
    time = float(line[0]) - start
    if time < min_time:
        min_time = time
    if time > max_time:
        max_time = time
    
    entries.append((time,float(line[1])))

# Discard warmup
thresh = WARMUP_FRAC * (max_time - min_time)
entries = [ (x, y) for (x, y) in entries if x > thresh ]

# Plots
plt.scatter(*zip(*entries), s=5)
plt.ylabel('Latency (s)')
plt.xlabel('Runtime (s)')
plt.show()

plt.hist([y for (x,y) in entries], bins=100)
plt.ylabel('Frequency')
plt.xlabel('Latency (s)')
plt.show()

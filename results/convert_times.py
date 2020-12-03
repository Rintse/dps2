#!/bin/python3
from sys import argv
import pprint

file = open(argv[1])
lines = file.read().splitlines()
data = [ l.split(',') for l in lines ]

data.sort(key=lambda x: float(x[0]))

start = -1.0
for line in data:
    if start == -1.0:
        start = float(line[0])

    print(float(line[0])-start, " ", line[1])

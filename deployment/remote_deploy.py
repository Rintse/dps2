#!/usr/bin/python3
import os
import sys
import subprocess

if len(sys.argv) <= 4:
    print("Must supply num_workers, init_workers, gen_rate and target cluster")
    exit()

# Syncs project folder with DAS5, before running the cluster
if os.getcwd().split("/")[-1] != "dps2":
    print("This script must be called from the project root dir")
else:
    print("Copying changes to the das... ", end='', flush=True)
    os.system("rsync -r --delete . "  + sys.argv[4] + ":DPS2")
    print("done.")

    print("Running remotely:")
    os.system( "ssh " + sys.argv[4] + " DPS2/deployment/run_bench.py " + \
                    sys.argv[1] + " " + sys.argv[2] + " " + sys.argv[3] )

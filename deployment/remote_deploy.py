#!/usr/bin/python3
import os
import sys
import subprocess

# Skip these directories when syncing with the das
exclude_dirs = [
    "storm_bench/run_libs",
    "storm_bench/compile_libs"
]

if len(sys.argv) < 3:
    print("Must supply num_workers and gen_rate")
    exit()

# Syncs project folder with DAS5, before running the cluster
if os.getcwd().split("/")[-1] != "dps2":
    print("This script must be called from the project root dir")
else:
    print("Copying changes to the das... ", end='')
    os.system(
        "rsync -r --delete " + \
        " ".join(["--exclude " + i for i in exclude_dirs]) + \
        " ./* das5:DPS2")
    print("done.")

    print("Running remotely:")
    os.system("ssh das5 DPS2/deployment/run_bench.py " + sys.argv[1] + " " + sys.argv[2])

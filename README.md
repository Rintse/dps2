# DPS assignment 2

This project very rudamentally mimics processing the data from a US presedential election.
We generate votes as {county, party} pairs, which we aggregate to obtain the republican and 
democrat votes. We then analyze these results and give some info(-graphics).

The project is subdivided into 7 components:

- **aggregator:** *A storm topology that aggregates votes by county* 
- **analyzer:** *A python loop that extracts various statistics from the aggregation data*
- **benchmark_driver:** *A python script that generates votes and spawns threaded sockets to send these*
- **configs:** *Config files for the various softwares used*
- **data:** *Large data files needed by some of the components*
- **deployment:** *Scripts to deploy the above tools onto the das5 cluster*
- **results:** *The results from benchmarking the cluster*

## Aggregator
A storm topology, built using the stream API. Receives the aforementioned pairs, and 
aggregates the votes for each party, grouped by county. Since our benchmarks of this 
cluster require us to measure latency, we also keep track of the event time 
of such aggregation operations. The vote aggregates are added to a total, kept in a 
mongodb database. The latencies are inserted into a seperate database (on a different node) 
to prevent our benchmarking from impacting the clusters performance.

## Analyzer
A python script that reads the total aggregates from the mongodb database. These results
are per-county. The analyzer uses this information to calculate results for each state, as 
well as the country as a whole. TODO: serve these results

## Benchmark driver
A python script that spawns threads to generate the aforementioned pairs. These pairs are then
sent to the Aggregator over multiple sockets (one for each worker node in the aggregator).

## Deployment
A script to deploy the various components of the cluster. Also has an interface to kill the
cluster in a controlled fashion.

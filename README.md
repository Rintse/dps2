# DPS assignment 2

This project very rudamentally mimics processing the data from the US presedential election.
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

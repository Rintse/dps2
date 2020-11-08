# Functions implementing the data "Generator" component.
# Contains functionality for a local NTP client if provided, else it uses system time.

from random import randrange
from time import sleep
from queue import Full as queueFullError
import ntplib
from numpy.random import normal

# .py
from our_ntp import getLocalTime
from streamer import STOP_TOKEN # To be put in an error queue in case a Generator encounters an exception.

# Data ranges for Tuple Generator.
GEM_RANGE   = 8
PRICE_RANGE = 5

# Queue parameters
PUT_TIMEOUT = 0

# Generates a random number from a normal distribution in range [lower_bound, upper_bound).
def rand_normal(mean, sd, lower_bound, upper_bound):
    return min( max( int( round( normal(mean, sd) ) ), 0), upper_bound-1)

# Tuple generator for a purchase-tuple = (GemId, price, event_time)
# Will generate the time stamp with NTP if a time_client is available, else it uses time.time()
def gen_purchase(time_client=None):
    purchase = (
        rand_normal((GEM_RANGE-1)/2, GEM_RANGE/4, 0, GEM_RANGE),
        randrange(PRICE_RANGE),
        getLocalTime(time_client)
    )
    return purchase

# Generator that performs calls to purchase tuple generator and fill the shared queue `q`.
# `budget` function calls are made at a rate of `rate` per second.
# If it can not put data in the `q`, it throws an exception and communicates this through the 
# shared `error` queue
def purchase_generator(q, error, time_client, id, rate, budget):
    print("Start purchase generator ", id)

    for _ in range(budget):
        start = getLocalTime(time_client)

        try:
            q.put(gen_purchase(time_client), PUT_TIMEOUT)
        except queueFullError as e:
            error.put(STOP_TOKEN)
            raise RuntimeError("\n\tGenerator-{} reached Queue threshold\n".format(id)) from e


        diff = getLocalTime(time_client) - start
        sleep(max(0, 1/rate - diff))

# Small test of generator functionality
if __name__ == "__main__":
    print("__________________\nTest ad_generator")

    # for i in range(10):
    #     print(gen_purchase())

    res = [rand_normal((GEM_RANGE-1)/2, GEM_RANGE/4, 0, GEM_RANGE) for i in range(10000)]
    dist = [res.count(i) for i in range(GEM_RANGE)]
    print(dist)

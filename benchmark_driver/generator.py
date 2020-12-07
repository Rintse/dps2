# Functions implementing the data "Generator" component.
# Contains functionality for a local NTP client if provided, else it uses system time.

from random import randrange
from time import sleep
from queue import Full as queueFullError
import ntplib
from numpy.random import normal
from time import time

# Data ranges for Tuple Generator.
GEM_RANGE   = 100
PRICE_RANGE = 5

STOP_TOKEN = "_STOP_"

# Queue parameters
PUT_TIMEOUT = 0

def state_list():
    return open("DPS2/data/states.dat", "r").read().splitlines()

# Generates a random number from a normal distribution in range [lower_bound, upper_bound).
def rand_normal(mean, sd, lower_bound, upper_bound):
    return min( max( int( round( normal(mean, sd) ) ), 0), upper_bound-1)

# gets a random element from list
def rand_from(target_list, size):
    return target_list[randrange(size)]

# generate a random vote
def gen_vote(states, n):
    return (rand_from(states, n), randrange(2), time())

def vote_generator(q, error, id, rate, budget, paused):
    print("Start vote generator ", id)

    states = state_list()
    n = len(states)

    for _ in range(budget):
        while paused.value:
            pass

        start = time()
        
        try:
            q.put(gen_vote(states, n), PUT_TIMEOUT)
        except queueFullError as e:
            error.put(STOP_TOKEN)
            raise RuntimeError("\n\tGenerator-{} reached Queue threshold\n".format(id)) from e

        diff = time() - start
        sleep(max(0, 1/rate - diff))

# Small test of generator functionality
if __name__ == "__main__":
    print("__________________\nTest ad_generator")

    states = state_list()
    n = len(states)

    for i in range(10):
        print(gen_vote(states, n))

    # res = [rand_normal((GEM_RANGE-1)/2, GEM_RANGE/4, 0, GEM_RANGE) for i in range(10000)]
    # dist = [res.count(i) for i in range(GEM_RANGE)]
    # print(dist)


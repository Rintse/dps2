# Functions implementing the data "Generator" component.
# Contains functionality for a local NTP client if provided, else it uses system time.

from random import randrange
from time import sleep
from queue import Full as queueFullError
import ntplib
from numpy.random import normal
from time import time

STOP_TOKEN = "_STOP_"
# Queue parameters
PUT_TIMEOUT = 0

def state_list():
    return open("DPS2/data/states.dat", "r").read().splitlines()

# gets a random element from list
def rand_from(target_list, size):
    return target_list[randrange(size)]

# generate a random vote
def gen_vote(states, n):
    return (rand_from(states, n), randrange(2), time())

def vote_generator(q, id, rate, budget, start):
    print("Start vote generator ", id)

    states = state_list()
    n = len(states)

    while not start.value:
        pass # Wait for start signal
    
    totalbegin = time()

    for _ in range(budget):
        begin = time()
        
        try:
            q.put(gen_vote(states, n), PUT_TIMEOUT)
        except queueFullError as e:
            q.put(STOP_TOKEN, PUT_TIMEOUT)
            raise RuntimeError("\n\tGenerator-{} reached Queue threshold\n".format(id)) from e

        diff = time() - begin
        sleep(max(0, 1/rate - diff))
    
    print("Total time generating: ", time()-totalbegin)
    # Signal end of stream
    q.put(STOP_TOKEN, PUT_TIMEOUT)


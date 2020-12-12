# Functions implementing the data "Generator" component.
# Contains functionality for a local NTP client if provided, else it uses system time.

from random import randrange
from time import sleep
from queue import Full as queueFullError
from numpy.random import normal
from time import time
from pprint import pprint

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
    
    time_per_gen = 1 / rate

    states_genned = { s : 0 for s in states }

    dem = 0
    rep = 0

    begin = time()
    for i in range(budget):
        
        # Keep stats of genned votes
        vote = gen_vote(states,n)
        if vote[1]:
            dem += 1
        else:
            rep += 1

        states_genned[vote[0]] += 1

        try:
            q.put((str(id)+"-"+str(i),) + vote, PUT_TIMEOUT)
        except queueFullError as e:
            q.put(STOP_TOKEN, PUT_TIMEOUT)
            raise RuntimeError("\n\tGenerator-{} reached Queue threshold\n".format(id)) from e

        runtime = time() - begin
        expected_runtime = (i+1) * time_per_gen
        sleep(max(0, expected_runtime - runtime))
    
    total_time = time() - begin

    #  pprint(states_genned)

    print("Generated {} tuples in {} seconds".format(budget, total_time))
    print("Actual generation rate: {}".format(budget/total_time))
    print("Genned {} republican votes".format(dem))
    print("Genned {} democratic votes".format(rep))
    # Signal end of stream
    q.put(STOP_TOKEN, PUT_TIMEOUT)


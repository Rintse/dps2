# Class that implements the "Streamer" component
# Takes arguments to direct an experiment
# Creates parallel processes running "Generators"
# Sends generated tuples over a TCP connection using a multiprocessing.Queue as buffer
from multiprocessing import Process, Queue, Lock, Value
import ctypes
from queue import Empty as queueEmptyError
from time import sleep
from time import time
from math import ceil
from sys import argv
import socket
import ntplib

#.py
import generator

STOP_TOKEN = "_STOP_" # Token send by a Generator if it encounters and exception
START_PORT = 5555

# Implements the "Streamer" component
class Streamer:
    # statics
    TEST = False                # generate data without TCP connection, to test data Generators
    PRINT_CONN_STATUS = True    # print messages regarding status of socket connection
    PRINT_CONFIRM_TUPLE = False # print tuples when they are being send
    PRINT_QUEUE_SIZES = True    # print the sizes of the queue during the run

    SOCKET_TIMEOUT = 6000       # how long the Streamer waits on a TCP connection
    HOST = "0.0.0.0"

    QUEUE_BUFFER_SECS = 5       # maximum size of the queue expressed in seconds of generation
    GET_TIMEOUT = 10            # how long the Streamer waits for a tuple from the queue (should never have to wait this long)

    QUEUE_LOG_INTERVAL = 0.5    # time in seconds between queuesize logs

    # Object variables
    #   q: Queue                -- buffer between the Generators and the SUT
    #   error_q: Queue          -- communicates error from child to Streamer
    #   generators: Process     -- populates data using several parallel processes
    #   budget: int             -- how many tuples will be generated 
    #   generation_rate: int    -- how many tuples will be generated per second
    #   results: [int]          -- list containing predicted aggregation results for each GemID
    #   q_size_log: [int]       -- list tracking the sizes of the queue at each iteration

    def __init__(self, port, budget, rate, n_generators, ntp_address):
        self.q = Queue(rate * self.QUEUE_BUFFER_SECS)
        print("Queue maxsize: {}".format(self.q._maxsize))

        self.error_q = Queue()
        self.budget = budget
        self.results = [0]*generator.GEM_RANGE
        self.q_size_log = []
        self.done_sending = Value(ctypes.c_bool, False)
        self.port = port

        sub_rate = rate/n_generators
        # ensure each generator creates enough, this slightly overestimates with at most n_generators
        # does not affect the amount of tuples sent over TCP
        sub_budget = ceil(budget/n_generators)

        # creates NTP clients if a host is provided
        if ntp_address == None:
            ntp_clients = [None] * n_generators
        else:
            ntp_clients = [ (ntplib.NTPClient(), ntp_address) ]  * n_generators

        # seperate thread to log the size of `q` at a time interval
        self.qsize_log_thread = Process(target=self.log_qsizes, args=())

        # initialize generator processes
        self.generators = [
            Process(target=generator.vote_generator,
            args=(self.q, self.error_q, (ntp_clients[i]), i, sub_rate, sub_budget,),
            daemon = True)
        for i in range(n_generators) ]


    # end -- def __init__

    def log_qsizes(self):
        if not self.PRINT_QUEUE_SIZES:
            return

        start = time()
        while not self.done_sending.value:
            print("|Q|@ ", time()-start, ":", self.q.qsize())
            sleep(self.QUEUE_LOG_INTERVAL)

        print("Time taken: {}".format(time()-start))

    # end -- def log_qsizes

    # starts stream to terminal if TEST otherwise over TCP
    def run(self):
        try:
            if self.TEST:
                self.stream_test()
            else:
                self.stream_from_queue()
        except:
            raise
        finally:
            if self.PRINT_CONFIRM_TUPLE:
                for i, r in enumerate(self.results):
                    print(i, ": ", r)

    # end -- def run

    # generates `self.budget` number of tuples and consumes them using the callable `consume_f` argument
    def consume_loop(self, consume_f, *args):
        for g in self.generators:
            g.start()

        for i in range(self.budget):
            data = self.get_purchase_data()

            if data == STOP_TOKEN:
                self.done_sending.value = True
                raise RuntimeError("Aborting Streamer, exception raised by generator")

            consume_f(data, i, *args)

    # end -- def consume_loop

    # runs the consume_loop, prints all generated tuples to output
    def stream_test(self):
        # consume_f function
        def print_to_terminal(data, i):
            if self.PRINT_CONFIRM_TUPLE:
                print("TEST{}: got".format(i), data)

        self.consume_loop(print_to_terminal, ())

    # end -- def stream_test

    # runs the consume_loop, sends all generated tuples over TCP connection
    def stream_from_queue(self):
        # consume_f function
        def send(data, i, c):
            c.sendall(data.encode())

            if self.PRINT_CONFIRM_TUPLE:
                print('Sent tuple #', i)

        # Start
        if self.PRINT_CONN_STATUS:
            print("Start Streamer")

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(self.SOCKET_TIMEOUT) 
            s.bind((self.HOST, self.port))
            s.listen(0)

            if self.PRINT_CONN_STATUS:
                print("waiting for connection ...")

            conn, addr = s.accept()

            with conn:
                if self.PRINT_CONN_STATUS:
                    print("Streamer connected by", addr)

                self.qsize_log_thread.start()
                self.consume_loop(send, conn)

                print("All tuples sent, waiting for cluster in recv ...")
                self.done_sending.value = True
                conn.recv(1)

    # end -- def stream_from_queue

    # attempts to get a tuple from `q`
    # if an error was raised in a generator, returns the STOP_TOKEN
    # else if it can read from the queue it converts the tuple into JSON format
    # returns JSON string or STOP_TOKEN
    def get_purchase_data(self):
        try: #check for errors from generators
            return self.error_q.get_nowait()
        except Exception:
            pass # There was no error raised

        try:
            (county, party, event_time) = self.q.get(timeout=self.GET_TIMEOUT)
        except queueEmptyError as e:
            raise RuntimeError('Streamer timed out getting from queue') from e

        vote = '{{ "county":{}, "party":{}, "event_time":{} }}\n'.format(county, 'D' if party else 'R', event_time)

        if self.PRINT_CONFIRM_TUPLE:
            self.results[county] += 1
            print(vote)

        return vote

    # end -- def get_purchase_data

# converts argument string to an integer
def arg_to_int(arg, name):
    try:
        return int(arg)
    except ValueError as e:
        raise RuntimeError('\n\t commandline argument of invalid type.\n\t`{}` must be of type `int`\n\tUse: `benchmark_driver.py [budget: uint] [generation_rate: uint] [n_generators: uint]`'.format(name)) from e
    except Exception:
        raise

if __name__ == "__main__":
    if len(argv) < 4:
        raise ValueError('\n\tToo few arguments.\n\tUse: `benchmark_driver.py [budget: uint] [generation_rate: uint] [n_generators: uint]`')

    budget       = arg_to_int(argv[1], "budget")
    rate         = arg_to_int(argv[2], "generation_rate")
    n_streamers = arg_to_int(argv[3], "n_streamers")
    ntp_address  = argv[4] if len(argv) > 4 else None

    drivers = [Streamer(START_PORT + i, budget, rate, 2, ntp_address) for i in range(n_streamers)]
    streamer_threads = [ Process(target=d.run, args=()) for d in drivers ]
    
    for thread in streamer_threads:
        thread.start()
    
    for thread in streamer_threads:
        thread.join()

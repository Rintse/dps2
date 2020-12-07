# Class that implements the "Streamer" component
# Takes arguments to direct an experiment
# Creates parallel processes running "Generators"
# Sends generated tuples over a TCP connection using a multiprocessing.Queue as buffer
from multiprocessing import Process, Queue, Lock, Value
import ctypes
from queue import Empty as queueEmptyError
from time import sleep
from datetime import datetime
from time import time
from math import ceil
from sys import argv
import socket
import ntplib

#.py
import generator

STOP_TOKEN = "_STOP_" # Token send by a Generator if it encounters and exception
START_PORT = 5555

N_GENERATORS = 1

# Implements the "Streamer" component
class Streamer:
    # statics
    TEST = False                # generateata without TCP connection, to test data Generators
    PRINT_CONN_STATUS = True    # print messages regarding status of socket connection
    PRINT_CONFIRM_TUPLE = True # print tuples when they are being send
    PRINT_QUEUE_SIZES = False   # print the sizes of the queue during the run

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

    def __init__(self, port, budget, rate, n_generators):
        self.q = Queue()
        self.error_q = Queue()
        self.budget = budget
        self.rate = rate
        self.n_generators = n_generators
        self.q_size_log = []
        self.done_sending = Value(ctypes.c_bool, False)
        self.paused = Value(ctypes.c_bool, True)
        self.port = port
        self.sent = 0

        # seperate thread to log the size of `q` at a time interval
        self.qsize_log_thread = Process(target=self.log_qsizes, args=())

    # end -- def __init__

    def init_generators(self):
        sub_rate = self.rate/self.n_generators
        # ensure each generator creates enough, this slightly overestimates with at most n_generators
        # does not affect the amount of tuples sent over TCP
        sub_budget = ceil(self.budget/self.n_generators)

        # initialize generator processes
        self.generators = [
            Process(target=generator.vote_generator,
            args=(self.q, self.error_q, i, sub_rate, sub_budget, self.paused,),
            daemon = True)
        for i in range(self.n_generators) ]

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
        self.init_generators()
        self.stream_from_queue()
    # end -- def run

    # generates `self.budget` number of tuples and consumes them using the callable `consume_f` argument
    def consume_loop(self, c):
        print(self.budget)

        if self.failed is not None:
            c.sendall(self.failed.encode())
        
        while self.sent < self.budget:
            data = self.get_data()

            if data == STOP_TOKEN:
                self.done_sending.value = True
                raise RuntimeError("Aborting Streamer, exception raised by generator")

            try:
                c.sendall(data.encode())
            except:
                self.failed = data
                print("Send failed")
                raise

            if self.PRINT_CONFIRM_TUPLE:
                print('Sent tuple #', self.sent)

            self.failed = None
            self.sent += 1
    # end -- def consume_loop

    # runs the consume_loop, sends all generated tuples over TCP connection
    def stream_from_queue(self):
        self.failed = None
        # Start
        if self.PRINT_CONN_STATUS:
            print(str(str(datetime.now())), " Start Streamer")

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(self.SOCKET_TIMEOUT) 
            bound = False
            while not bound:
                try:
                    s.bind((self.HOST, self.port))
                    bound = True
                except OSError:
                    print("Address already in use, retrying in 3s") 
                    sleep(3)

            s.listen(0)

            for g in self.generators:
                g.start()

            self.qsize_log_thread.start()
            
            # Loop for connections in case of fail
            while self.sent < self.budget:
                try:
                    if self.PRINT_CONN_STATUS:
                        print("waiting for connection ...")

                    conn, addr = s.accept()
                    self.paused.value = False

                    with conn:
                        if self.PRINT_CONN_STATUS:
                            print("Streamer connected by", addr)

                        self.consume_loop(conn)

                except KeyboardInterrupt:
                    break
                except Exception as e:
                    print(e)
                    print("Socket disconnected, retrying")
                    self.paused.value = True

            print("All tuples sent, waiting for cluster in recv ...")
            self.done_sending.value = True
            conn.recv(1)

    # end -- def stream_from_queue

    # attempts to get a tuple from `q`
    # if an error was raised in a generator, returns the STOP_TOKEN
    # else if it can read from the queue it converts the tuple into JSON format
    # returns JSON string or STOP_TOKEN
    def get_data(self):
        try: #check for errors from generators
            return self.error_q.get_nowait()
        except Exception:
            pass # There was no error raised

        try:
            (state, party, event_time) = self.q.get(timeout=self.GET_TIMEOUT)
        except queueEmptyError as e:
            raise e

        vote = '{{ "state":"{}", "party":"{}", "event_time":{} }}\n'.format(state, 'D' if party else 'R', event_time)

        if self.PRINT_CONFIRM_TUPLE:
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

    budget      = arg_to_int(argv[1], "budget")
    rate        = arg_to_int(argv[2], "generation_rate")
    port        = arg_to_int(argv[3], "port")
    
    Streamer(port, budget, rate, 1).run()


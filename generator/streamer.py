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
import select

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
    PRINT_CONFIRM_TUPLE = False # print tuples when they are being send
    PRINT_QUEUE_SIZES = False   # print the sizes of the queue during the run

    SOCKET_TIMEOUT = 6000       # how long the Streamer waits on a TCP connection
    HOST = "0.0.0.0"

    QUEUE_LOG_INTERVAL = 0.5    # time in seconds between queuesize logs

    # Object variables
    #   q: Queue                -- buffer between the Generators and the SUT
    #   error_q: Queue          -- communicates error from child to Streamer
    #   generator: Process      -- populates data using a parallel process
    #   budget: int             -- how many tuples will be generated 
    #   generation_rate: int    -- how many tuples will be generated per second
    #   results: [int]          -- list containing predicted aggregation results for each GemID
    #   q_size_log: [int]       -- list tracking the sizes of the queue at each iteration

    def __init__(self, port, budget, rate):
        self.q = Queue()
        self.budget = budget
        self.rate = rate
        self.q_size_log = []
        self.done_sending = Value(ctypes.c_bool, False)
        self.start = Value(ctypes.c_bool, False)
        self.port = port
        self.sent = 0
        
        self.generator = Process(target=generator.vote_generator,
            args=(self.q, self.port, self.rate, self.budget, self.start,),
            daemon = True
        )

        # seperate thread to log the size of `q` at a time interval
        self.qsize_log_thread = Process(target=self.log_qsizes, args=())
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
        self.stream_from_queue()
    # end -- def run

    # generates `self.budget` number of tuples and consumes them using the callable `consume_f` argument
    def consume_loop(self, c):
        print(self.budget)

        if self.failed is not None:
            c.sendall(self.failed.encode())
            self.sent += 1
        
        while self.sent < self.budget:
            data = self.get_data()

            # Since the get is blocking, getting None here means end of stream 
            if data == None:
                return

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
            print(str(datetime.now()), " Start Streamer")

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

            self.generator.start()
            self.qsize_log_thread.start()
            
            # Loop for connections in case of fail
            while self.sent < self.budget:
                try:
                    if self.PRINT_CONN_STATUS:
                        print("waiting for connection ...")

                    conn, addr = s.accept()
                    self.start.value = True

                    with conn:
                        if self.PRINT_CONN_STATUS:
                            print("Streamer connected by", addr)

                        self.consume_loop(conn)
                        
                        self.done_sending.value = True
                        self.wait_for_cluster(conn)

                except KeyboardInterrupt:
                    break
                except socket.error:
                    print("Socket disconnected, retrying")

    # end -- def stream_from_queue

    
    # Waits for the cluster by recving until an EOF is received
    def wait_for_cluster(self, conn):
        print("Done sending {} tuples, waiting for cluster to DIE".format(self.sent))
        print(conn)
        try:
            while True:
                select.select([], [conn,], [], 5)
                sleep(1)
        except select.error:
            print("Cluster shut down, closing")
    # end -- def wait_for_cluster


    # attempts to get a tuple from `q`
    # if an error was raised in a generator, returns the STOP_TOKEN
    # else if it can read from the queue it converts the tuple into JSON format
    # returns JSON string or STOP_TOKEN
    def get_data(self):
        data = self.q.get() # blocking get
        if data == STOP_TOKEN:
            return None
        else:
            (id, state, party, event_time) = data

        vote = '{{ "id":"{}", "state":"{}", "party":"{}", "event_time":{} }}\n'.format(id, state, 'D' if party else 'R', event_time)

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
    
    Streamer(port, budget, rate).run()


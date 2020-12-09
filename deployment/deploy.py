import os
import time
import sched
import threading
import ctypes

# Indexation of available_nodes
ZK_NIMBUS_IDX   = 0
GENERATOR_IDX   = 1
MONGO_DATA_IDX  = 2
LATENCY_WEB_IDX = 3
WORKER_IDX      = 4

TOPOLOGY_NAME = "agsum"

# Parameters
BUDGET = 6 * 10 * 1000
BASE_PORT = 5555
IB_SUFFIX = ".ib.cluster"
AUTO_SHUTDOWN_MINS = 14
ROOT = "/home/ddps2016/DPS2/"

# Configs
STORM_TEMPLATE = ROOT + "configs/storm/storm-template.yaml"
STORM_CONFIG = ROOT + "configs/storm/storm.yaml"
MONGO_CONFIG = ROOT + "configs/mongo/mongodb.conf"
ZOOKEEPER_CONFIG_DIR = ROOT + "configs/zookeeper"

# Data locations
EMPTY_MONGO = "/var/scratch/ddps2016/mongo_data/"
EMPTY_LAT_MONGO = "/var/scratch/ddps2016/mongo_lat_data/"
MONGO_DATA = "/local/ddps2016/mongo_data"
STORM_DATA = "/local/ddps2016/storm-local"
RESULTS_DIR = ROOT + "results"

# Log locations
STORM_LOGS = "/local/ddps2016/storm-logs"
ZOOKEEPER_LOGS = "/home/ddps2016/zookeeper/logs"
MONGO_LOGS = "/home/ddps2016/mongo/log"

# Program locations
DATA_GENERATOR = ROOT + "benchmark_driver/streamer.py"

# Export libs to screen
SCREEN_LIBS = "export LD_LIBRARY_PATH_SCREEN=$LD_LIBRARY_PATH;"


# Helper for gen_config_file: generate worker list
def worker_list(worker_nodes):
    w_list = ""
    for i in worker_nodes:
        w_list += i + IB_SUFFIX
        if i is not worker_nodes[-1]:
            w_list += ","
    return w_list

# Helper for kill: generate name for results
def result_name(num_workers, gen_rate):
    return RESULTS_DIR + "/" + str(gen_rate) + "_" + str(num_workers) + "node.res"

# Helper to parse cli input
def isInt(s):
    try: 
        int(s)
        return True
    except ValueError:
        return False


# Class that manages a run on the cluster
class RunManager:
    def __init__(self, available_nodes, i_workc, gen_rate):
        # Not enough nodes
        assert len(available_nodes) > WORKER_IDX
        # Too many initial workers given
        assert len(available_nodes[WORKER_IDX:]) >= i_workc

        self.gen_rate           = gen_rate
        # Node allocation
        self.zk_nimbus_node     = available_nodes[ZK_NIMBUS_IDX]
        self.generator_node     = available_nodes[GENERATOR_IDX]
        self.mongo_data_node    = available_nodes[MONGO_DATA_IDX]
        self.latency_web_node   = available_nodes[LATENCY_WEB_IDX]
        self.worker_nodes       = available_nodes[WORKER_IDX:]
        self.cur_workers        = []
        self.worked             = [] #The nodes that have been a worker

        # Automatically shut down the cluster before reservation ends
        self.autokill_timer = threading.Timer(
            AUTO_SHUTDOWN_MINS*60, self.kill_cluster, args=(True, False)    
        )
        self.autokill_timer.start()

        # Lock to ensure shutdown is performed only once
        self.dead = False
        self.lock = threading.Lock()

    def deploy(self, init_num_workers):
        # Deploy mongo servers
        self.deploy_mongo(self.mongo_data_node, EMPTY_MONGO)
        self.deploy_mongo(self.latency_web_node, EMPTY_LAT_MONGO)
        # Generate a config file, because cli options do not work for some reason
        self.gen_storm_config_file()
        # Deploy storm cluster
        self.deploy_zk_nimbus()
       
        for port in range(BASE_PORT, BASE_PORT + len(self.worker_nodes)):
            self.deploy_new_streamer(port)

        # Deploy init_num_workers supervisors and streamers
        for node in self.worker_nodes[0:init_num_workers]:
            self.deploy_new_supervisor(on=node)

        time.sleep(3)
        # Submit topology to the cluster
        self.submit_topology()
        # Print overview of cluster 
        self.print_node_allocation()

    def print_node_allocation(self):
        r = len(self.worker_nodes)*10
        print("Nimbus/Zoo node:".ljust(20), str(self.zk_nimbus_node).ljust(r))
        print("Mongo data node:".ljust(20), str(self.mongo_data_node).ljust(r))
        print("Latency/web node:".ljust(20), str(self.latency_web_node).ljust(r))
        print("Generator node:".ljust(20), str(self.generator_node).ljust(r))
        print("Worker nodes:".ljust(20), str(self.worker_nodes).ljust(r))
        print("Current workers:".ljust(20), str(self.cur_workers).ljust(r))

    # Needs to all be set in config file, because the cli settings don't work well
    def gen_storm_config_file(self):
        os.system(
            "cat " + STORM_TEMPLATE + " | sed \'"
            "s/NIM_SEED/" + self.zk_nimbus_node + IB_SUFFIX + "/g; " + \
            "s/ZOO_SERVER/" + self.zk_nimbus_node + IB_SUFFIX + "/g; " + \
            "s/SUPERVISORS/" + worker_list(self.cur_workers) + "/g" + \
            "\' > " + STORM_CONFIG )
    
    # Submits topology (should be called after cluster is initted)
    def submit_topology(self):
        submit_command = \
            "cd " + ROOT + "aggregator; make submit" + \
            " STORM_CONF=" + STORM_CONFIG + \
            " INPUT_ADRESS=" + self.generator_node + IB_SUFFIX + \
            " INPUT_PORT=5555" + \
            " MONGO_ADRESS=" + self.mongo_data_node + IB_SUFFIX + \
            " MONGO_LAT_ADRESS=" + self.latency_web_node + IB_SUFFIX + \
            " NUM_WORKERS=" + str(len(self.cur_workers)) + \
            " NUM_STREAMS=" + str(len(self.worker_nodes)) + \
            " GEN_RATE=" + str(self.gen_rate)

        print("Submitting topology to the cluster")
        os.system(submit_command)

    # Deploys a mongo database server
    def deploy_mongo(self, node, initial_data):
        print("Copying mongo files to node", node)
        os.system(
            "ssh " + node + " 'mkdir -p " + MONGO_DATA + "; " + \
            "rsync -r --delete " + initial_data + " " + MONGO_DATA + "'"
        )

        mongo_start_command = " 'screen -d -m numactl " + \
            "--interleave=all mongod --config " + MONGO_CONFIG + "'"

        print("Deploying mongo server on " + node)
        os.system("ssh " + node + mongo_start_command)

    # Deploys the zookeeper server, and a storm nimbus on the same node
    def deploy_zk_nimbus(self):
        # Start the zookeeper server
        zk_start_command = " 'zkServer.sh --config " + \
            ZOOKEEPER_CONFIG_DIR + " start" + "'"
        os.system("ssh " + self.zk_nimbus_node + zk_start_command)

        # Create local storage folder
        os.system("ssh " + self.zk_nimbus_node + " 'mkdir -p " + STORM_DATA + "'")
        os.system("ssh " + self.zk_nimbus_node + " 'mkdir -p " + STORM_LOGS + "'")

        #Start the nimbus
        nimbus_start_command = " '" + SCREEN_LIBS + \
            " screen -d -m storm nimbus --config " + STORM_CONFIG + \
            " -c storm.local.hostname=" + self.zk_nimbus_node + IB_SUFFIX + "'"

        print("Deploying nimbus on " + self.zk_nimbus_node)
        os.system("ssh " + self.zk_nimbus_node + nimbus_start_command)

    # Deploys a streamer that streams to @param node
    def deploy_new_streamer(self, port):
        print("Deploying new streamer to stream to {}".format(port))

        generator_start_command = " '" + SCREEN_LIBS + \
            " screen -d -m -S streamer" + str(port) + " -L python3 " + \
            DATA_GENERATOR + " " + str(int(BUDGET/len(self.worker_nodes))) + \
            " " + str(self.gen_rate) + " " + str(port) + "'"

        os.system("ssh " + self.generator_node + generator_start_command)

    # Kills the streamer that streamed to @param node 
    def kill_streamer(self, node):
        index = self.worker_nodes.index(node)

        kill_command = " 'screen -S {} -X quit".format("streamer" + index)
        os.system("ssh " + self.generator_node + kill_command)

    # Deploys a supervisor on @param node
    def deploy_new_supervisor(self, on):
        assert len(self.cur_workers) < len(self.worker_nodes)

        self.worked.append(on)
        self.cur_workers.append(on)

        # Create local storage folder
        os.system("ssh " + on + " 'mkdir -p " + STORM_DATA + "'")
        os.system("ssh " + on + " 'mkdir -p " + STORM_LOGS + "'")
        
        worker_start_command = " '" + SCREEN_LIBS + \
            " screen -d -m storm supervisor --config " + STORM_CONFIG + \
            " -c storm.local.hostname=" + on + IB_SUFFIX + "'"
 
        print("Deploying supervisor on node " + on)
        os.system("ssh " + on + worker_start_command)

    # Kills the supervisor on @param node
    def kill_supervisor(self, on):
        print("Killing supervisor on {}".format(on))
        # Kills the screen in which the supervisor is running
        os.system("ssh " + on + " 'killall screen'")
        # Mark node available again
        self.cur_workers.remove(on)

    # Add or remove nodes to pool
    def scale_nodes(self):
        print("Not implemented")

    # Rebalances the topology. Should be called after new supervisors are spawned
    def rebalance(self):
        # Generate a new config file including the new workers
        self.gen_storm_config_file()
            
        # Issue the rebalnce
        os.system(
            "storm rebalance --config {} -n {} {}".format(
                STORM_CONFIG, len(self.cur_workers), TOPOLOGY_NAME
            )
        )

    # Scale number of workers up using available nodes
    def scale_up(self, count):
        if len(self.cur_workers) + count > len(self.worker_nodes):
            print("Not enough available nodes")
            return
        
        cur_idx = len(self.cur_workers)

        for node in self.worker_nodes[cur_idx : cur_idx + count]:
            self.deploy_new_supervisor(node)

        self.rebalance()

    # Scale number of workers down using available nodes
    def scale_down(self, count):
        if len(self.cur_workers) - count < 1:
            print("Must retain at least one worker")
            return
    
        to_kill = self.cur_workers[-count:]

        for node in to_kill: 
            self.kill_supervisor(node)

        self.rebalance()

    def scale(self, _in):
        if not isInt(_in[3:]):
            print("Give an integer")
            return

        num = int(_in[3:])
        if num == 0:
            return

        if _in[1] == "-":
            self.scale_down(num)

        elif _in[1] == "+":
            self.scale_up(num)

        else:
            print("Must supply +/-")

    # Kills all screen instances on the storm nodes
    def kill_mongo(self):
        os.system("ssh " + self.mongo_data_node + " 'killall screen'")
        os.system("ssh " + self.latency_web_node + " 'killall screen'")
    
    # Kills all screen instances on the storm nodes
    def kill_storm(self):
        os.system("ssh " + self.zk_nimbus_node + " 'rm -rf " + STORM_DATA + "/*'")
        os.system("ssh " + self.zk_nimbus_node + " 'killall screen'")

        for i in self.cur_workers:
            os.system("ssh " + i + " 'rm -rf " + STORM_DATA + "/*'")
            os.system("ssh " + i + " 'killall screen'")

    # Cleans logs of storm, zookeeper and mongo
    def clean_logs(self):
        os.system("ssh " + self.zk_nimbus_node + " 'rm -rf " + STORM_LOGS + "/*'")
        # Clean all nodes that have been a worker
        for i in self.worked: 
            os.system("ssh " + i + " 'rm -rf " + STORM_LOGS + "/*'")

        os.system("rm " + ZOOKEEPER_LOGS + "/*")
        os.system("rm " + MONGO_LOGS + "/*")

    # Kills the cluster in a contolled fashion
    def kill_cluster(self, autokill, keep_logs):
        # Make sure kill is only called once
        self.lock.acquire()
        if self.dead:
            if not autokill: # Autokill happened, join autokill process
                self.autokill_timer.join()
            self.lock.release()
            return
        self.dead = True
        if not autokill: # Main got here first, terminate autokill process
            self.autokill_timer.cancel()
        self.lock.release()

        print("Killing cluster{}, {} logs.".format(
            " automatically" if autokill else "", "keeping" if keep_logs else "discarding"
        ))

        # Kill the topology
        os.system("storm kill --config " + STORM_CONFIG + " " + TOPOLOGY_NAME)
        print("Spouts disabled. Waiting 5 seconds to process leftover tuples")
        time.sleep(5)

        # Export mongo latency data
        os.system(
            "mongoexport --host " + self.latency_web_node + " -u storm -p test -d " + \
            "results -c latencies -f \"time,latency\" " + \
            "--type=csv -o " + result_name(len(self.worker_nodes), self.gen_rate)
        )

        # Kill and clean local data of storm cluster and local mongo folders
        self.kill_storm()
        self.kill_mongo()

        # Clean mongo data
        os.system("ssh " + self.mongo_data_node + " 'rm -rf " + MONGO_DATA + "/*'")
        os.system("ssh " + self.latency_web_node + " 'rm -rf " + MONGO_DATA + "/*'")

        # Clean logs
        if not keep_logs:
            self.clean_logs()

        # Reset zookeeper storm files in zookeeper
        os.system("zkCli.sh -server " + self.zk_nimbus_node + ":2186 deleteall /storm")
        # Cancel reservation
        os.system("preserve -c $(preserve -llist | grep ddps2016 | cut -f 1)")
        
        if autokill: # Print that shutdown was done automatically
            print("Automatic shutdown successful. Press enter to quit")
        

    # Runs an interactive interface to control the cluster
    def run_interface(self):
        # Wait for kill command, or kill automatically if out of time
        while not self.dead:
            _in = input(
                "\n\"k[y/n]\" to kill the cluster keeping or discarding logs\n" + \
                "\"s+ n\" to scale up with n nodes\n" + \
                "\"s- n\" to scale down with n nodes\n" + \
                "\"p\" to print the current node allocation\n\n"
            )

            if _in == "":
                if self.dead:
                    break # Quit if already autokilled

            elif _in[0] == "k":
                if len(_in) < 2 or (len(_in) >= 2 and not (_in[1] == "y" or _in[1] == "n")):
                    print("Must specify y or n (keep logs?)")
                    continue
                self.kill_cluster(False, True if _in[1] == "y" else False)

            elif _in[0] == "s":
                self.scale(_in)

            elif _in[0] == "p":
                self.print_node_allocation()


# Deploy the cluster
def deploy_all(available_nodes, init_num_workers, gen_rate, reservation_id):
    # Initialize the manager with the resources from the reservation
    run_manager = RunManager(available_nodes, init_num_workers, gen_rate)
    # Deploy the cluster
    run_manager.deploy(init_num_workers)
    # Run an interface with which to scale/kill the cluster
    run_manager.run_interface()
            

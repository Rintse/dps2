import os
import time
import sched
import multiprocessing
import ctypes

ZK_NIMBUS_IDX   = 0
GENERATOR_IDX   = 1
MONGO_DATA_IDX  = 2
LATENCY_WEB_IDX = 3
WORKER_IDX      = 4

# Parameters
BUDGET = 1000000
IB_SUFFIX = ".ib.cluster"
AUTO_SHUTDOWN_MINS = 13.5
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
    def __init__(self, avail, i_workc, gen_rate):
        # Not enough nodes
        assert len(avail) > WORKER_IDX
        # Too many initial workers given
        assert len(avail[WORKER_IDX:]) > i_workc

        self.gen_rate           = gen_rate
        self.num_workers        = 0

        # Node allocation
        self.zk_nimbus_node     = avail[ZK_NIMBUS_IDX]
        self.generator_node     = avail[GENERATOR_IDX]
        self.mongo_data_node    = avail[MONGO_DATA_IDX]
        self.latency_web_node   = avail[LATENCY_WEB_IDX]
        self.worker_nodes       = avail[WORKER_IDX:]
        self.cur_workers        = []
        self.worked             = [] #The nodes that have been a worker

        # Start backgroud process to 
        # automatically shut down the cluster
        self.autokill_proc = multiprocessing.Process(
            target=self.auto_shutdown, args=()
        )
        self.autokill_proc.start()
       
        # Lock to ensure shutdown is performed only once
        self.dead = multiprocessing.Value(ctypes.c_bool, False)
        self.lock = multiprocessing.Lock()

    def deploy(self):
        # Deploy mongo servers
        self.deploy_mongo(self.mongo_data_node, EMPTY_MONGO)
        self.deploy_mongo(self.latency_web_node, EMPTY_LAT_MONGO)
        # Generate a config file, because cli options do not work for some reason
        self.gen_storm_config_file()
        # Deploy data input generator
        self.deploy_generator()
        # Deploy storm cluster
        self.deploy_zk_nimbus()
        self.deploy_workers()
        # Submit topology to the cluster
        self.submit_topology()

    # Needs to all be set in config file, because the cli settings don't work well
    def gen_storm_config_file(self):
        os.system(
            "cat " + STORM_TEMPLATE + " | sed \'"
            "s/NIM_SEED/" + self.zk_nimbus_node + IB_SUFFIX + "/g; " + \
            "s/ZOO_SERVER/" + self.zk_nimbus_node + IB_SUFFIX + "/g; " + \
            "s/SUPERVISORS/" + worker_list(self.worker_nodes) + "/g" + \
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
            " NUM_WORKERS=" + str(self.num_workers) + \
            " GEN_RATE=" + str(self.gen_rate)

        print("Submitting topology to the cluster")
        os.system(submit_command)

    # Deploys the custom data generator
    def deploy_generator(self):
        # Start in screen to check output (only program that does not log to file)
        generator_start_command = " '" + SCREEN_LIBS + \
            " screen -d -m -L python3 " + DATA_GENERATOR + \
            " " + str(BUDGET) + " " + str(self.gen_rate) + \
            " " + str(self.num_workers) + "'"

        print("Deploying generator on " + self.generator_node)
        os.system("ssh " + self.generator_node + generator_start_command)

    # Deploys a mongo database server
    def deploy_mongo(self, node, initial_data):
        print("Copying mongo files to node", node)
        os.system(
            "ssh " + node + " 'mkdir -p " + MONGO_DATA + "; " + \
            "rsync -r --delete " + initial_data + " " + MONGO_DATA + "'"
        )

        mongo_start_command = " 'screen -d -m numactli " + \
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

    def deploy_new_worker(self):
        assert len(self.cur_workers) < len(self.worker_nodes)

        # Get the new worker
        self.num_workers += 1
        new_worker = self.worker_nodes[self.num_workers]
        self.worked.append(new_worker)
        self.cur_workers.append(new_worker)

        # Create local storage folder
        os.system("ssh " + new_worker + " 'mkdir -p " + STORM_DATA + "'")
        os.system("ssh " + new_worker + " 'mkdir -p " + STORM_LOGS + "'")
        
        worker_start_command = " '" + SCREEN_LIBS + \
            " screen -d -m storm supervisor --config " + STORM_CONFIG + \
            " -c storm.local.hostname=" + new_worker + IB_SUFFIX + "'"
 
        print("Deploying supervisor on node " + new_worker)
        os.system("ssh " + new_worker + worker_start_command)

    # Deploys the storm supervisors
    def deploy_workers(self):
        for _ in range(self.num_workers):
            self.deploy_new_worker()

    # Automatic shutdown function
    def auto_shutdown(self):
        s = sched.scheduler(time.time, time.sleep)
        s.enter(AUTO_SHUTDOWN_MINS*60, 1, self.kill_cluster, argument=(True,False))
        s.run(True)    

    # Scaling functions
    def scale_up(self, count):
        print("Not implemented")

    def scale_down(self, count):
        print("Not implemented")

    def scale(self, _in):
        num = int(_in[3:])
        if not isInt(num):
            print("Give an integer")
            return

        if _in[1] == "-":
            self.scale_down(int(num))
        elif _in[1] == "+":
            self.scale_up(int(num))
        else:
            print("Must supply +/-")

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
        if self.dead.value:
            if not autokill: # Autokill happened, join autokill process
                self.autokill_proc.join()
            self.lock.release()
            return
        self.dead.value = True
        if not autokill: # Main got here first, terminate autokill process
            self.autokill_proc.terminate()
        self.lock.release()   

        print("Killing cluster{}.".format(" automatically" if autokill else ""))

        # Kill the topology
        os.system("storm kill --config " + STORM_CONFIG + " agsum")
        print("Spouts disabled. Waiting 5 seconds to process leftover tuples")
        time.sleep(5)

        # Export mongo latency data
        os.system(
            "mongoexport --host " + self.latency_web_node + " -u storm -p test -d " + \
            "results -c latencies -f \"time,latency\" " + \
            "--type=csv -o " + result_name(len(self.worker_nodes), self.gen_rate)
        )

        # Kill and clean local data of storm cluster
        self.kill_storm()

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
            print("Automatic shutdown successful")
        

    # Runs an interactive interface to control the cluster
    def run_interface(self):
        # Wait for kill command, or kill automatically if out of time
        while not self.dead.value:
            _in = input(
                "Type:\n" + \
                "\"k\"[y/n] to kill the cluster keeping or discarding logs\n" + \
                "\"s+ n\" to scale up with n nodes\n" + \
                "\"s- n\" to scale down with n nodes\n"
            )

            # Check if we were automatically killed
            if _in[0] == "k":
                if len(_in) < 3 or (len(_in) >= 3 and not (_in[2] == "y" or _in[2] == "n")):
                    print("Must specify y or n (keep logs?)")
                    continue
                self.kill_cluster(False, True if _in[2] == "y" else False)
            elif _in[0] == "s":
                self.scale(_in)


def deploy_all(available_nodes, init_num_workers, gen_rate, reservation_id):
    # Initialize the manager with the resources from the reservation
    run_manager = RunManager(available_nodes, init_num_workers, gen_rate)
    # Deploy the cluster
    run_manager.deploy()
    # Run an interface with which to scale/kill the cluster
    run_manager.run_interface()
            

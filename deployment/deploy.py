import os
import time
import sched
import multiprocessing
import ctypes


# Global lock to ensure shutdown is performed only once
dead = multiprocessing.Value(ctypes.c_bool, False)
lock = multiprocessing.Lock()

# Parameters
BUDGET = 200000
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

# Deploys the zookeeper server, and a storm nimbus on the same node
def deploy_zk_nimbus(node, worker_nodes):
    # Start the zookeeper server
    zk_start_command = " 'zkServer.sh --config " + ZOOKEEPER_CONFIG_DIR + " start" + "'"
    os.system("ssh " + node + zk_start_command)
    time.sleep(2)    

    # Create local storage folder
    os.system("ssh " + node + " 'mkdir -p " + STORM_DATA + "'")
    os.system("ssh " + node + " 'mkdir -p " + STORM_LOGS + "'")

    #Start the nimbus
    nimbus_start_command = " '" + SCREEN_LIBS + \
        " screen -d -m storm nimbus --config " + STORM_CONFIG + \
        " -c storm.local.hostname=" + node + IB_SUFFIX + "'"

    print("Deploying nimbus on " + node)
    os.system("ssh " + node + nimbus_start_command)


# Deploys the storm supervisors
def deploy_workers(nodes, zk_nimbus_node):
    for i in nodes:
        # Create local storage folder
        os.system("ssh " + i + " 'mkdir -p " + STORM_DATA + "'")
        os.system("ssh " + i + " 'mkdir -p " + STORM_LOGS + "'")
        
        worker_start_command = " '" + SCREEN_LIBS + \
            " screen -d -m storm supervisor --config " + STORM_CONFIG + \
            " -c storm.local.hostname=" + i + IB_SUFFIX + "'"
 
        print("Deploying supervisor on node " + i)
        os.system("ssh " + i + worker_start_command)


# Deploys the custom data generator
def deploy_generator(node, num_workers, gen_rate, reservation_id):
    # Start in screen to check output (only program that does not log to file)
    generator_start_command = " '" + SCREEN_LIBS + \
        " screen -d -m -L python3 " + DATA_GENERATOR + " " + \
        str(BUDGET) + " " + str(gen_rate) + " " + str(num_workers) + "'"

    print("Deploying generator on " + node)
    os.system("ssh " + node + generator_start_command)


# Deploys the mongo database server
def deploy_mongo(node, initial_data):
    print("Copying mongo files to node")
    os.system(
        "ssh " + node + " 'mkdir -p " + MONGO_DATA + "; " + \
        "rsync -r --delete " + initial_data + " " + MONGO_DATA + "'"
    )

    mongo_start_command = \
	" 'screen -d -m numactl --interleave=all" + \
	" mongod --config " + MONGO_CONFIG + " &'"

    print("Deploying mongo server on " + node)
    os.system("ssh " + node + mongo_start_command)


# Submits topology to the cluster
def submit_topology(
    nimbus_node, 
    generator_node, 
    mongo_node,
    mongo_latency_node,
    num_workers, 
    worker_nodes, 
    gen_rate
):
    print("Waiting for the storm cluster to initialize...")
    time.sleep(10)
    
    submit_command = \
            "cd " + ROOT + "aggregator; make submit" + \
        " STORM_CONF=" + STORM_CONFIG + \
        " INPUT_ADRESS=" + generator_node + IB_SUFFIX + \
        " INPUT_PORT=5555" + \
        " MONGO_ADRESS=" + mongo_node + IB_SUFFIX + \
        " MONGO_LAT_ADRESS=" + mongo_latency_node + IB_SUFFIX + \
        " NUM_WORKERS=" + str(num_workers) + \
        " GEN_RATE=" + str(gen_rate)

    print("Submitting topology to the cluster")
    os.system(submit_command)


# Helper for gen_config_file: generate worker list
def worker_list(worker_nodes):
    w_list = ""
    for i in worker_nodes:
        w_list += i + IB_SUFFIX
        if i is not worker_nodes[-1]:
            w_list += ","
    return w_list

# Needs to all be set in config file, because the cli settings don't work well
def gen_storm_config_file(zk_nimbus_node, worker_nodes):
    os.system(
        "cat " + STORM_TEMPLATE + " | sed \'"
        "s/NIM_SEED/" + zk_nimbus_node + IB_SUFFIX + "/g; " + \
        "s/ZOO_SERVER/" + zk_nimbus_node + IB_SUFFIX + "/g; " + \
        "s/SUPERVISORS/" + worker_list(worker_nodes) + "/g" + \
        "\' > " + STORM_CONFIG)


# Helper for kill: generate name for results
def result_name(num_workers, gen_rate):
    return RESULTS_DIR + "/" + str(gen_rate) + "_" + str(num_workers) + "node.res"

# Kills the cluster in a contolled fashion
def kill_cluster(
    zk_nimbus_node,
    mongo_node,
    mongo_latency_node,
    worker_nodes,
    gen_rate,
    autokill
):
    global dead
    global lock

    lock.acquire()
    if dead.value:
        lock.release()
        return
    dead.value = True 
    lock.release()   

    print("Killing cluster{}.".format(" automatically" if autokill else ""))

    # Kill the topology
    os.system("storm kill --config " + STORM_CONFIG + " agsum")
    print("Spouts disabled. Waiting 5 seconds to process leftover tuples")
    time.sleep(5)

    # Reset zookeeper storm files
    os.system("zkCli.sh -server " + zk_nimbus_node + ":2186 deleteall /storm")

    # Export mongo data
    os.system(
        "mongoexport --host " + mongo_latency_node + " -u storm -p test -d " + \
        "results -c latencies -f \"time,latency\" " + \
        "--type=csv -o " + result_name(len(worker_nodes), gen_rate)
    )

    # Cancel reservation
    os.system("preserve -c $(preserve -llist | grep ddps2016 | cut -f 1)")
    
    # Clean storm local storage
    os.system("ssh " + zk_nimbus_node + " 'rm -rf " + STORM_DATA + "/*'")
    for i in worker_nodes:
        os.system("ssh " + i + " 'rm -rf " + STORM_DATA + "/*'")

    # Clean mongo data
    os.system("ssh " + mongo_node + " 'rm -rf " + MONGO_DATA + "/*'")
    os.system("ssh " + mongo_latency_node + " 'rm -rf " + MONGO_DATA + "/*'")

    # Prompt to clean logs
    if autokill or input("Clean logs?\n") == "y":
        os.system("ssh " + zk_nimbus_node + " 'rm -rf " + STORM_LOGS + "/*'")
        for i in worker_nodes:
            os.system("ssh " + i + " 'rm -rf " + STORM_LOGS + "/*'")

        os.system("rm " + ZOOKEEPER_LOGS + "/*")
        os.system("rm " + MONGO_LOGS + "/*")

    if autokill:
        print("Automatic shutdown successful, press enter continue")


def auto_shutdown(
    zk_nimbus_node,
    mongo_node, 
    mongo_latency_node, 
    worker_nodes, 
    gen_rate
):
    s = sched.scheduler(time.time, time.sleep)
    s.enter(
        AUTO_SHUTDOWN_MINS*60, 1, kill_cluster, 
        argument=(zk_nimbus_node, mongo_node, 
        mongo_latency_node, worker_nodes, gen_rate, True,)
    )
    s.run(True)    


def deploy_all(available_nodes, gen_rate, reservation_id):
    global dead
    global lock
    assert len(available_nodes) > 3
    
    # Assign nodes
    zk_nimbus_node = available_nodes[0]
    generator_node = available_nodes[1]
    mongo_data_node = available_nodes[2]
    mongo_latency_node = available_nodes[3]
    worker_nodes = available_nodes[4:]
    num_workers = len(worker_nodes)

    # Set up a timer to close everything neatly after 15 minutes
    p = multiprocessing.Process(
        target=auto_shutdown, 
        args=(zk_nimbus_node, mongo_data_node, 
        mongo_latency_node, worker_nodes,gen_rate)
    )
    p.start()
    
    # Deploy mongo servers
    deploy_mongo(mongo_data_node, EMPTY_MONGO)
    deploy_mongo(mongo_latency_node, EMPTY_LAT_MONGO)
    
    # Generate a config file, because cli options do not work for some reason
    gen_storm_config_file(zk_nimbus_node, worker_nodes)
    
    # Deploy data input generator
    deploy_generator(generator_node, num_workers, gen_rate, reservation_id)
    
    # Deploy storm cluster
    deploy_zk_nimbus(zk_nimbus_node, worker_nodes)
    deploy_workers(worker_nodes, zk_nimbus_node)

    # Submit topology to the cluster
    submit_topology(
        zk_nimbus_node, 
        generator_node, 
        mongo_data_node,
        mongo_latency_node,
        num_workers, 
        worker_nodes, 
        gen_rate
    )

    # Wait for kill command, or kill automatically if out of time
    while True:
        _in = input("Type \"k\" to kill the cluster\n")

        lock.acquire()
        if dead.value:
            p.join()
            lock.release()
            break
        elif _in == "k":
            p.terminate()
            lock.release()
            kill_cluster(
                zk_nimbus_node, 
                mongo_data_node, 
                mongo_latency_node,
                worker_nodes, 
                gen_rate, 
                False
            )
            break
        else:
            lock.release()
            

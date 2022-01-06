#!/usr/bin/python3.8

#####################################################################################################################
# This script is to execute bgsave sequentially to avoid temoporary yet perhaps significant latency load on cluster
# The action plan is first, we will see master node of  
#
#
#
#
#####################################################################################################################

from rediscluster import RedisCluster
import sys, argparse
import platform
import subprocess
import time


parser = argparse.ArgumentParser() #Initialize parser
parser.add_argument("-a","--auth",help="masterauth to get an access to cluster.",required=True)
parser.add_argument("-b","--bootstrap-server",help="bootstrap-server list for cluster dicovery",nargs="+",required=True) # To get list of argument
args = parser.parse_args()
auth=args.auth
servers=args.bootstrap_server

def connector():
    rc="" #initialize
    startup_nodes = [f"redis://:{auth}@{x}" for x in servers]
    for protocol in startup_nodes:
        try :
            rc= RedisCluster.from_url(protocol, decode_responses =True)
            #If succeeded,
            return rc
        except Exception as e:
            pass
    else:
        print("No connection available")
        sys.exit(1)

def background_save(redis_cluster):
    print("Background will begin to save a backup...")
    for instance in redis_cluster.info().keys():
        if platform.system() == "Linux": 
            print(f"Node : {instance} runs background save...")
            subprocess.run(f"redis-cli -a {auth} -h {instance[:-4]} -p {instance[-5:]} bgsave 2>/dev/null",shell=True)
            time.sleep(3)
        else:
            pass
    else:
        sys.exit(0)



if __name__ == "__main__":
    rc = connector()
    background_save(rc)
    






#!/usr/bin/python3.8

#####################################################################################################################
# This script is to paginate the Redis cluster to find keys without using a command, 
# "keys", as the command imposes quite a heavy load on server 
# 
# Exaplantion on 'mode' argument 
# f - finding all keys for a given prefix
# d - dectecting keys that are not conforming to prefix rules and count them all.
#
#
# Explanation on 'usecase' argument
# farm0 - RedisFarm0
# farma - RedisFarmA
# farmb1 - RedisFarmB1
# farmb2 - RedisFarmB2
#####################################################################################################################

from rediscluster import RedisCluster
import redis 
import argparse
import csv
from pathos.multiprocessing import ProcessingPool as Pool
import sys
from collections import deque
from elasticsearch import helpers

parser=argparse.ArgumentParser()
parser.add_argument("-p","--prefix",required=True,help="Specify prefix with which the key start.")
parser.add_argument("-b","--bootstrap_server",required=True,help="Specify the path to serverlist file")
parser.add_argument("-a","--auth",required=True)
parser.add_argument("-m","--mode",required=True) 
parser.add_argument("-u","--usecase",required=True)

args=parser.parse_args()
auth=args.auth
servers=args.bootstrap_server
prefix=args.prefix
mode=args.mode
usecase=args.usecase

def get_serverlist():
    server_list=[]
    with open(servers,"r") as f:
        csv_reader=csv.DictReader(f)
        for server in csv_reader:
            server_list.append(server[usecase])
    return server_list

def connector():
    startup_nodes = [ f"redis://:{auth}@{x}" for x in get_serverlist() ]
    for protocol in startup_nodes:
        try :
            rc= RedisCluster.from_url(protocol, decode_responses =False)
            #If succeeded,
            return rc
        except Exception as e:
            pass
    else:
        print("[ERROR] No connection available")   
        sys.exit(1)

def get_session():
    cluster_con = connector()
    masters=list(cluster_con.scan().keys())
    master_sessions = [redis.from_url(f"redis://:{auth}@{x}") for x in masters]
    return master_sessions


def get_prefixes():
    prefixes = {}
    with open("prefixes.csv","r") as f:
        csv_reader = csv.DictReader(f)
        for row in csv_reader:
            if row[usecase] != "":
                prefixes[str(bytes(row[usecase],"utf-8")).split(":")[0]]=True #True == 1 
    return prefixes

def key_finder():
    session = connector()
    for key in session.scan_iter(match=f"{prefix}*"):
        print(key)


def illegal_keys(session):  
    prefix_dict=get_prefixes()
    for key in session.scan_iter():
        if not prefix_dict.get(str(key).split(":")[0],False):
            print(f"[ERROR] key '{key}' violoates prefix rules ")
            prefix_dict[str(key).split(":")[0]]=True
        else:
            prefix_dict[str(key).split(":")[0]]+=1
    print(prefix_dict)

def all_illegal_keys(master_sessions):
    with Pool(3) as executor:
        results = executor.map(illegal_keys,master_sessions)
    for result in results:
        print(result)

if __name__ == "__main__":
    if mode == "f":
        key_finder()
    elif mode == "d":
        all_illegal_keys(get_session())


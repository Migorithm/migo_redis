#!/usr/bin/python3.8

#####################################################################################################################
# This script is to paginate the Redis cluster to find keys without using a command, 
# "keys", as the command imposes quite a heavy load on server 
# 
# Exaplantion on 'mode' argument 
# f - finding all keys for a given prefix
# d - dectecting keys that are not conforming to prefix rules
# c - finding all keys and count them
#
#
# Explanation on 'usecase' argument
# farma - RedisFarmA
# farmb - RedisFarmB
# farm0 - RedisFarm0
#####################################################################################################################

from rediscluster import RedisCluster 
import argparse
import csv
import concurrent.futures
import threading

parser=argparse.ArgumentParser()
parser.add_argument("-p","--prefix",required=True,help="Specify prefix with which the key start.")
parser.add_argument("-b","--bootstrap_server",nargs="+",required=True)
parser.add_argument("-a","--auth",required=True)
parser.add_argument("-m","--mode",required=True) 
parser.add_argument("-u","--usecase",required=True)

args=parser.parse_args()
auth=args.auth
servers=args.bootstrap_server
prefix=args.prefix
mode=args.mode
usecase=args.usecase

thread_local=threading.local()

def connector():
    startup_nodes = [ f"redis://:{auth}@{x}" for x in servers ]
    for protocol in startup_nodes:
        try :
            rc= RedisCluster.from_url(protocol, decode_responses =True)
            #If succeeded,
            return rc
        except Exception as e:
            pass
    else:
        print("[ERROR] No connection available")   
        sys.exit(1)

def get_session():
    if not hasattr(thread_local,"session"):
        thread_local.session = connector()
    return thread_local.session

def get_prefixes():
    prefixes = []
    with open("prefixes.csv","r") as f:
        csv_reader = csv.DictReader(f)
        for row in csv_reader:
            prefixes.append(row[usecase])
    return prefixes

def key_finder():
    session = get_session()
    for key in session.scan_iter(match=f"{prefix}*"):
        print(key)

def illegal_keys(prefixes):  
    session=get_session()
    for key in session.scan_iter():
        for comparatives in prefixes:
            if key.startswith(comparatives):
                break
        else:
            print(f"[ERROR] key '{key}' violoates prefix rules")


def key_counter(prefix):
    session=get_session()
    cnt=0
    for key in session.scan_iter(match=f"{prefix}*"):
        cnt += 1 
    print(f"{prefix} : {cnt}")

def all_key_counter(list_of_prefixes):
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(key_counter,list_of_prefixes)

    
if __name__ == "__main__":
    if mode == "f":
        key_finder()
    elif mode == "d":
        illegal_keys(get_prefixes())
    elif mode == "c":            
        all_key_counter(get_prefixes())

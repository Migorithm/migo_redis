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

class RedisKeyManager:
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

    def get_serverlist(self):
        server_list=[]
        with open(self.servers,"r") as f:
            csv_reader=csv.DictReader(f)
            for server in csv_reader:
                server_list.append(server[self.usecase])
        return server_list

    def connector(self):
        startup_nodes = [ f"redis://:{self.auth}@{x}" for x in self.get_serverlist() ]
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

    def get_session(self):
        cluster_con = self.connector()
        masters=list(cluster_con.scan().keys())
        master_sessions = [redis.from_url(f"redis://:{self.auth}@{x}") for x in masters]
        return master_sessions


    def get_prefixes(self):
        prefixes = {}
        with open("prefixes.csv","r") as f:
            csv_reader = csv.DictReader(f)
            for row in csv_reader:
                if row[self.usecase] != "":
                    prefixes[str(bytes(row[self.usecase],"utf-8")).split(":")[0]]=True #True == 1 
        return prefixes


    def key_finder(self,session):
        for key in session.scan_iter(match=f"{self.prefix}*"):
            print(key)

    def all_key_finder(self,master_sessions):
        with Pool(3) as executor:
            executor.map(self.key_finder,master_sessions)


    def illegal_keys(self,session):  
        prefix_dict=self.get_prefixes()
        prefix_dict["violation"] = []
        for key in session.scan_iter():
            if not prefix_dict.get(str(key).split(":")[0],False):
                print(f"[ERROR] key '{str(key)[2:-2]}' violoates prefix rules ")
                prefix_dict["violation"].append(str(key).split(":")[0])
                prefix_dict[str(key).split(":")[0]]=True
            else:
                prefix_dict[str(key).split(":")[0]]+=1
        for k,v in prefix_dict.items():
            print(f"[COUNT] {k[2:]} {v}")

    def all_illegal_keys(self,master_sessions):
        with Pool(3) as executor:
            executor.map(self.illegal_keys,master_sessions)
        

if __name__ == "__main__":
    key_manager=RedisKeyManager()
    if key_manager.mode == "f" and key_manager.prefix:
        key_manager.all_key_finder(key_manager.get_session())
    elif key_manager.mode == "d":
        key_manager.all_illegal_keys(key_manager.get_session())

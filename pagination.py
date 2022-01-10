#!/usr/bin/python3.8

#####################################################################################################################
# This script is to paginate the Redis cluster to find keys without using a command, 
# "keys", as the command imposes quite a heavy load on server 
# 
# 
#
#
#####################################################################################################################

from rediscluster import RedisCluster 
import argparse

parser=argparse.ArgumentParser()
parser.add_argument("-p","--prefix",required=True,help="Specify prefix with which the key start.")
parser.add_argument("-b","--bootstrap_server",nargs="+",required=True)
parser.add_argument("-a","--auth",required=True)
args=parser.parse_args()
auth=args.auth
servers=args.bootstrap_server
prefix=args.prefix



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
        print("[ERROR] No connection available")   
        sys.exit(1)

def key_finder(connector):
    con = connector
    for key in con.scan(match=f"{prefix}*"):
        print(key)


if __name__ == "__main__":
    con = connector()
    key_finder(con)

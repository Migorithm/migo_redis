#!/usr/bin/python3.8

#####################################################################################################################
# This script is to execute bgsave upon trigger point signalled to the server. 
# Plus, alarm will be sent to Telegram API so DB admin gets to know something wrong is happening in the server.
# 
# 
#
#
#####################################################################################################################

#logrotate   -> /etc/logrotate.d/  
# /var/lib/redis/6379/*.rdb {
# rotate 3
# missing ok 
# create 640 root root
# }   --> minimum interval you can set is hour. 

from rediscluster import RedisCluster
import redis
import sys, argparse
import platform
import time
import datetime
import requests
import json


parser = argparse.ArgumentParser() #Initialize parser
parser.add_argument("-a","--auth",help="masterauth to get an access to cluster.",required=True)
parser.add_argument("-b","--bootstrap_server",help="bootstrap-server list for cluster dicovery",nargs="+",required=True) # To get list of argument
args = parser.parse_args()
auth=args.auth
servers=args.bootstrap_server
load_json=json.load(open("./telekey.json"))
key= load_json.get("KEY")
chat_id=load_json.get("CHAT_ID")

telegram_url=f"https://api.telegram.org/bot{key}/sendMessage?parse-mod=html&chat_id={chat_id}"


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

def telegram(connetectable,servers):
    message={"text":""}
    for instance in servers:
        if instance not in connetectable:
            message["text"]=f"[ERROR] Redis Instance '{instance}' not connectable."
            requests.post(telegram_url,message)


def trigger_save(redis_cluster):
    if len(redis_cluster.info().keys()) != len(servers):
        print("[ERROR] Node count not match.")

        #Send a message
        telegram(redis_cluster.info().keys(),servers)

        for instance in redis_cluster.info().keys():
            role= redis_cluster.info()[instance].get("role",False) 
            if role == "master" and platform.system() == "Linux": 
                try_count=0
                while try_count <3:
                    print(f"[PROCESS] Master node '{instance}' attempts to run background save...")
                    current_time=datetime.datetime.now().strftime("%Y-%h-%d:%H.%M.%S")
                    connection = redis.from_url(f"redis://:{auth}@{instance}")
                    success=None #initialize
                    if connection.config_set("dbfilename",f"{current_time}.rdb"):
                        success=connection.bgsave()
                    if not success:
                        print(f"[ERROR] Background saving process in '{instance}' failed. Check out the server")
                        try_count+=1
                        time.sleep(0.5)
                        continue
                    else :
                        print(f"[SUCCESS] Background saving process in '{instance}' Success.")
                        time.sleep(0.5)
                        break
                else:
                    print("[ERROR] BGSAVE operation against master nodes failed.") 
            else:
                pass
    else:
        sys.exit(0)



if __name__ == "__main__":
    rc = connector()
    trigger_save(rc)
    



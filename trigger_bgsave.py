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
# /var/log/redis/*.rdb {
# rotate 3
# missing ok 
# create 640 root root
# }   --> minimum interval you can set is hour. 

#connection method change...
#rdb file name change and save logic... 
# telegram 



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
    startup_nodes = [f"redis://password@{x}" for x in servers]
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

def trigger_save(redis_cluster):
    if len(redis_cluster.info().keys()) != len(servers):
        print("[ERROR] Node count not match.")
        for instance in redis_cluster.info().keys():
            master= redis_cluster.info()[instance].get("slave0",False)  # role : master
            if master and platform.system() == "Linux": 
                try_count=0
                while try_count <3:
                    #naming rc.config_set("dbfilename","datetime"), while using 'logrotate' to delete old rdb.
                    print(f"[PROCESS] Master node '{instance}' attempts to run background save...")
                    run=subprocess.run(f"redis-cli -a {auth} -h {master[:-4]} -p {master[-4:]} bgsave 2>/dev/null",shell=True)
                    if run.returncode != 0:
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
    



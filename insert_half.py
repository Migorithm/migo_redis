#!/usr/bin/env python

#####################################################################################################################
# This script is to insert about half the system memory data into redis cluster to test things out
#
#
#
#
#
#####################################################################################################################

import random
import string
import uuid 
import psutil
from rediscluster import RedisCluster

rc="" #initialize
startup_nodes= ["redis://:password@ip1:port1","redis://:password@ip1:port1","redis://:password@ip1:port1"]
for protocol in startup_nodes:
    try :
        rc= RedisCluster.from_url(protocol, decode_responses =True)
        break
    except Exception as e:
        pass
else:
    print("No connection available")

MEM_GB = int(psutil.virtual_memory().total/1024**3)
KEY_SIZE=1024**2
TOTAL_KEYS = int(MEM_GB *0.5 / KEY_SIZE)

def gen_data():
    return ''.join([random.choice(string.ascii_letters+string.digits) for x in range(1024)]* 1024) # size of 1mb

for i in range(TOTAL_KEYS): #So it becomes 1gb
    rc.set(str(uuid.uuid4()),gen_data())
#!/usr/bin/python3.8
#####################################################################################################################
# This script is to manage a number of rdb files on Redis server. 
# Cron job OR Systemd registration and some modification on source is required.
# 
#
#
#
#####################################################################################################################


import argparse
import os
from pathlib import Path

parser=argparse.ArgumentParser()
parser.add_argument("-f","--file",help="Path to Redis configuration file")
parser.add_argument("-d","--directory",help="Path to directory that contains config files")
args=parser.parse_args()

if args.file:
    #call a context manager
    with open(args.file) as f:
        for line in f:
            if line.startswith("dir"):
                directory=line[4:].rstrip()
    paths = sorted(filter(lambda x:str(x).endswith(".rdb"),Path(directory).iterdir()), key=os.path.getmtime,reverse=True) #reverse order already.
    while len(paths) > 3:
        os.remove(str(paths[-1]))
        paths.pop()
    os.exit(0)

elif args.directory:
    file_list=[]
    for file in Path(args.directory).iterdir():
        if "63" in file.name:
            file_list.append(str(file)) #add absolute path to the list
    for conf in file_list:
        with open(args.file) as f:
            for line in f:
                if line.startswith("dir"):
                    directory=line[4:].rstrip()
        paths = sorted(filter(lambda x:str(x).endswith(".rdb"),Path(directory).iterdir()), key=os.path.getmtime,reverse=True) # Oldest one would come last
        while len(paths) > 3:
            os.remove(str(paths[-1]))
            paths.pop()
    else:
        os._exit(0)
else:
    os._exit(1)


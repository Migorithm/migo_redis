#!/usr/bin/python3.8

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
    paths = sorted(filter(lambda x:str(x).endswith(".rdb"),Path(directory).iterdir()), key=os.path.getmtime) #reverse order already.
    while len(paths) > 3:
        os.remove(str(paths[0]))

elif args.directory:
    file_list=[]
    for file in Path(args.directory).iterdir():
        if file.name.startswith("redis"):
            file.append(str(file))
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
    os._exit(1)


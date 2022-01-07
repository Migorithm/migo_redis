#!/bin/bash
#####################################################################################################################
# This script is to manage a number of rdb files on Redis server. 
# Cron job is required.
# 
#
#
#
#####################################################################################################################


main(){
CONF_FILE=($(ls $1 | grep ".*63.*.conf"))
for conf in ${CONF_FILE[@]};do 
    RDB_DIRECTORY=$(grep -P "dir " $1/$conf | grep -v "tls-ca-cert-dir" | awk '{gsub("dir ","",$0);print $0}')
    NUMBER_OF_RDB=$(ls $RDB_DIRECTORY| grep ".*rdb" | wc -l)
    while [[ $NUMBER_OF_RDB -gt 3 ]]; do
        rm $(ls $RDB_DIRECTORY --sort=time --reverse | grep "*.rdb" | head -1 )
        NUMBER_OF_RDB=$(ls $RDB_DIRECTORY| grep ".*rdb" | wc -l)
    done
done
}

if [[ -d $1 ]]; then
    main $1
fi
#!/bin/zsh

for i in {1..10} ; do
    ssh -o BatchMode=yes -o ConnectTimeout=5 "fa25-cs425-14$(printf %02d "$i").cs.illinois.edu" "cp vm${i}.logbk vm${i}.log"
    echo "finished VM ${i}"
done
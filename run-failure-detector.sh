#!/bin/zsh
processName="g14-mp2"

for i in {1..10}; do
  host="fa25-cs425-14$(printf %02d "$i").cs.illinois.edu"

  ssh -o BatchMode=yes -o ConnectTimeout=5 "$host" "pkill -f $processName"

  ssh -o BatchMode=yes -o ConnectTimeout=5 "$host" "nohup \"\$HOME/go/bin/${processName}\" > /dev/null 2>&1 & disown || true"

  echo "finished VM ${i}"
done
echo "finished ${processName}"
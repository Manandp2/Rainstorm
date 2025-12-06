#!/bin/zsh
mpnum="4"
for i in {1..10} ; do
  echo "working on vm${i}"
    script="
      if [ -d "g14-mp${mpnum}" ]; then
        cd g14-mp${mpnum} && git pull
      else
        git clone git@gitlab.engr.illinois.edu:manandp2/g14-mp${mpnum}.git && cd g14-mp${mpnum}
      fi;
      go mod download && go install ./...;
    "
    # shellcheck disable=SC2029
    ssh fa25-cs425-14"$(printf %02d "$i")".cs.illinois.edu "bash -c '${script}'"
done
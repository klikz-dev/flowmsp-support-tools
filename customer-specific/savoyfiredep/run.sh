#!/bin/bash -eu

PATH=$PATH:$HOME/repos/flowmsp/support-tools:$(cd $(dirname $0); pwd)
SLUG=$(basename $(pwd))

if [ $# -lt 1 ]
then
    envs="prod dev"
else
    envs=$*
fi

for e in $envs ; do
    echo e=$e
    dispatches.py flowmsp-$e $SLUG > ${e}_dispatches.json

    pull.py < ${e}_dispatches.json > ${e}_${SLUG}.csv
    subset.py lat lon < ${e}_${SLUG}.csv | awk '! /^,$/ { print $0}' > tamu_${e}.csv
done

compare.py prod_dispatches.json dev_dispatches.json > results.csv

exit 0


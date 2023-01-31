#!/bin/bash -eu

PATH=$PATH:$(cd $(dirname $0); pwd)

time flow_stats.py flowmsp-prod locations hydrants images dispatches --verbose > flow_stats.csv

exit 0



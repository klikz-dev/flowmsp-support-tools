#!/bin/bash -eu

(
head -1 existing_hydrants.csv

subset.py \
    lat \
    lon \
    _id \
    batchNo \
    createdBy \
    createdOn \
    flow \
    color \
    modifiedBy \
    modifiedOn \
    size \
    streetAddress < \
    existing_hydrants.csv | \
    awk 'NR > 1' | \
    sort -t , -k 1,1n -k 2,2n )

exit 0

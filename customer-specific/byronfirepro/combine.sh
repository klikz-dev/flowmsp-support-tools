#!/bin/bash -eu


(
    cat existing_hydrants_reformatted.csv
    cat hydrant_locations_combined.csv
) | \
    ./combine.py > t.csv

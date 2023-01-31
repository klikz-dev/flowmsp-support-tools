#!/bin/bash -eu

PATH=$PATH:$(cd $(dirname $0); pwd)

if [ $# -lt 2 ]
then
    printf "Usage: %s [ms-data-file] [state-abbrev]\n" $(basename $0) >&2
    exit 1
fi

INFILE=$1 ST_ABBREV=$2

ST=$(echo $ST_ABBREV | tr '[A-Z]' '[a-z]')

load_ms_geodata.py flowmsp-prod \
		   $INFILE \
		   ms_geodata_${ST} \
		   --verbose \
		   --create-index \
		   --truncate \
		   --db-url 'mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb'

exit 0

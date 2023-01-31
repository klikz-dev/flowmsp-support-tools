#!/bin/bash
set -eux

#------------------------------------------------------------
# geocode
#------------------------------------------------------------
geocode ()
{
    geocode.py flowmsp-prod "Street Address" City State "Zip code" < "$FILENAME" > ${GEOCODED}.tmp
    mv ${GEOCODED}.tmp $GEOCODED
}

#------------------------------------------------------------
# attach_polygons
#------------------------------------------------------------
attach_polygons ()
{
    attach_footprint.py flowmsp-prod $COLLECTION \
			--db-url $DB_URL \
			< $GEOCODED ${FOOTPRINTS}.tmp ${NOFOOTPRINTS}.tmp

    mv ${FOOTPRINTS}.tmp $FOOTPRINTS
    mv ${NOFOOTPRINTS}.tmp $NOFOOTPRINTS
}

#------------------------------------------------------------
# load_polygons
#------------------------------------------------------------
load_polygons ()
{
    mongo $DB_URL > collection.list <<EOF
use FlowMSP
show collections
EOF

    if ! grep "^${COLLECTION}" collection.list
    then 
	gzfile=${STATE_LC}.geojson.gz
	datafile=$(basename $gzfile .gz)

	aws --profile flowmsp-prod s3 cp ${DST}/$gzfile .
	gunzip $gzfile

	load_ms_geodata.py flowmsp-prod \
		       $datafile \
		       $COLLECTION \
		       --verbose \
		       --create-index \
		       --truncate \
		       --db-url $DB_URL

	rm -f $gzfile $datafile collection.list
    fi
}

#------------------------------------------------------------
# main
#------------------------------------------------------------
if [ $# -lt 2 ]
then
    printf "Usage: %s [state-abrv] [filename]\n" $(basename $0) >&2
    exit 1
fi

ST="$1" FILENAME="$2"
STATE_LC=$(echo "$ST" | tr '[A-Z]' '[a-z]')
COLLECTION=ms_geodata_${STATE_LC}
DST=s3://flowmsp-prod-doug/Microsoft/USBuildingFootprints
PATH=$PATH:$HOME/repos/support-tools
DB_URL="mongodb://127.0.0.1:27017"
INAME="$(basename "$FILENAME" .csv)"
ONAME="$(echo "$INAME" | tr '[A-Z]' '[a-z]' | sed -e 's/ /_/g')"
GEOCODED=${ONAME}_geocoded.csv
FOOTPRINTS=${ONAME}_geocoded_polygons.csv
NOFOOTPRINTS=${ONAME}_geocoded_nopolygons.csv

load_polygons
geocode
attach_polygons

exit 0

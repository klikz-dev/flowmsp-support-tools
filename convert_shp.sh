#!/bin/bash -eu

get_abs ()
{
    shp=$1

    python3 -c 'import os ; import sys; print(os.path.abspath(sys.argv[1]))' $shp
}

if [ $# -lt 1 ]
then
    printf "Usage: %s [shp-file...]\n" $(basename $0) >&2
    exit 1
fi

#: ${WDIR:=$(mktemp -d)}

#cd $WDIR

for shp in $*; do
    infile=/tmp/$(basename $shp)
    outfile=/tmp/$(basename $shp .shp).geojson

    echo \
    docker run --rm -v /tmp:$(pwd) osgeo/gdal:ubuntu-full-latest ogr2ogr \
	   -f geoJSON \
	   -t_srs crs:84 \
	   $outfile $infile
done

exit 0




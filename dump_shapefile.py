#!/usr/bin/env python3

import sys
import argparse
import csv
import time
import shapefile
import pyproj
from pyproj import CRS
import geojson


def get_time():
    ts = time.localtime()
    return time.strftime("%Y-%m-%d %H:%M:%S", ts)


def get_shapename(s):
    names = {"POLYGON": "Polygon", "POINT": "Point"}

    t = s.shape.shapeTypeName

    return names[t]


def get_coordinates(s, no_conversion):
    if s.shape.shapeTypeName == "POLYGON":
        coords = []
        c = []
        
        for x, y in s.shape.points:
            if no_conversion:
                lon = x
                lat = y
            else:
                lat, lon = pyproj.transform(src, dst, x, y)

            c.append([lon,lat])
        coords.append(c)
        
    else:
        x, y = s.shape.points[0]

        if no_conversion:
            lon = x
            lat = y
        else:
            lat, lon = pyproj.transform(src, dst, x, y)

        coords = [lon, lat]

    return coords
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="check for broken links")
    parser.add_argument("shapefile")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--no-conversion", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--report-interval", type=int, default=1000)

    args = parser.parse_args()
    sf = shapefile.Reader(args.shapefile)

    dst = CRS.from_string("epsg:4326")
    src = CRS.from_string("ESRI:102671")

    hdr = [h[0] for h in sf.fields[1:]]

    features = []

    records = sf.shapeRecords()

    print("%s: loaded %d shape records" %
          (get_time(), len(records)), file=sys.stderr)
    count = 0

    for snum, s in enumerate(records):
        if args.limit and snum >= args.limit:
            break

        count += 1

        if args.verbose and count == args.report_interval:
            count = 0
            print("%s: snum=%d" % (get_time(), snum), file=sys.stderr)

        coords = get_coordinates(s, args.no_conversion)

        orec = {
            "type": "feature",
            "geometry": {
                "type": get_shapename(s),
                "coordinates": coords,
            },
            "properties": {}
        }

        for k,v in zip(hdr, s.record):
            orec["properties"][k] = v

        features.append(orec)

    collection = geojson.FeatureCollection(features)
    print("found %d features" % len(features), file=sys.stderr)
    geojson.dump(collection, sys.stdout)

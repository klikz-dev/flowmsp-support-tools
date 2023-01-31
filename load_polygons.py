#!/usr/bin/env python3

import os
import sys
import json
import time
import argparse
import urllib.parse
import requests
import shapely
import shapely.geometry
import geojson


# https://nominatim.openstreetmap.org/search.php?q=glen+carbon%2C+IL&polygon_geojson=1&format=json
base_url = "https://nominatim.openstreetmap.org/search.php"

def get_geojson(place):
    url = "{base}?q={place}&polygon_geojson=1&format=json".format(
        base=base_url,
        place=urllib.parse.quote(place.replace(" ", "+"), safe="+")
    )
    # url = "https://nominatim.openstreetmap.org/search.php?q=glen+carbon%2C+IL&polygon_geojson=1&format=json"
    print("url=%s" % url, file=sys.stderr)

    r = requests.get(url)

    if r.ok:
        return r.json()


def get_geofence(geometry):
    print(geometry["display_name"], file=sys.stderr)

    return shapely.geometry.asShape(geometry["geojson"])


def within_polygon(loc, polygon):
    # polygon = shapely.geometry.asShape(feature["geometry"])

    return polygon.contains(loc["Point"])


def now():
    return time.strftime("%Y-%m-%d %H:%M:%S")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="load MS polygons for geographic area")
    parser.add_argument("place")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--progress", action="store_true")
    parser.add_argument("--count", type=int, default=100000)

    args = parser.parse_args()

    r = get_geojson(args.place)

    gf = get_geofence(r[0])

    building_data = geojson.load(sys.stdin)
    print("loaded %d features" % len(building_data["features"]), file=sys.stderr)

    features = []
    count = found = 0

    for i, f in enumerate(building_data["features"]):
        count += 1

        if args.limit and i >= args.limit:
            break

        if args.progress and count == args.count:
            print(now(), "features=%d in_geofence=%d" % (i + 1, found),
                  file=sys.stderr)
            count = 0

        polygon = shapely.geometry.asShape(f["geometry"])

        if gf.contains(polygon):
            features.append(f)
            found += 1

    print(now(), "features=%d in_geofence=%d" % (i + 1, found),
          file=sys.stderr)

    collection = geojson.FeatureCollection(features)

    geojson.dump(collection, sys.stdout)


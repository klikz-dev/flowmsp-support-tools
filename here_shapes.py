#!/usr/bin/env python3

import os
import sys
import csv
import time
import argparse
import requests
import pprint
import geojson
import shapely.wkt
import json

# https://developer.here.com/documentation/batch-geocoder/topics/endpoints.html

app_id="WaGiLAKzJQzySaXFJm9o"
app_code="bO4lKri2pJuiCxnRuD0-KQ"

base_url = "https://geocoder.api.here.com/6.2/geocode.json"

# Convert to a shapely.geometry.polygon.Polygon object
def wkt_to_geojson(s):
    x = s["Response"]["View"][0]["Result"][0]["Location"]["Shape"]["Value"]
    g1 = shapely.wkt.loads(x)
    g2 = geojson.Feature(geometry=g1, properties={})
    return g2.geometry


def get_shape(search, level):
    url = "{b}?app_id={id}&app_code={code}".format(b=base_url, id=app_id, code=app_code)
    url += "&searchtext={s}".format(s=search)
    url += "&additionaldata=IncludeShapeLevel,{level}".format(level=level)

    r = requests.get(url)

    return r

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="check for broken links")
    parser.add_argument("place")
    parser.add_argument("level",
                        choices=["country", "state", "county", "city",
                                 "district", "postalCode", "default"])
    parser.add_argument("--verbose", action="store_true")

    #parser.add_argument("profile", help="aws profile",
    #                    choices=["flowmsp-prod", "flowmsp-dev"])

    args = parser.parse_args()

    r = get_shape(args.place, args.level)

    if r.ok:
        data = r.json()

        if args.verbose:
            pprint.pprint(data)

        result = wkt_to_geojson(data)

        json.dump(result, sys.stdout)

        

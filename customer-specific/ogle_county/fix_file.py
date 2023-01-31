#!/usr/bin/env python3

import sys
import geojson
import pprint

if __name__ == "__main__":
    data = geojson.load(sys.stdin)

    features = [data["features"][f] for f in data["features"]]
    collection = geojson.FeatureCollection(features)
    geojson.dump(collection, sys.stdout)

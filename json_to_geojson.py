#!/usr/bin/env python3

import sys
import json
import geojson

if __name__ == "__main__":
    features = json.load(sys.stdin)
    collection = geojson.FeatureCollection(features)

    geojson.dump(collection, sys.stdout)

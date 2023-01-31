#!/usr/bin/env python3

import os
import sys
import argparse
import geojson

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="filter by tags")
    parser.add_argument("tags", nargs="+")

    args = parser.parse_args()
    data = geojson.load(sys.stdin)
    print("loaded %d features" % len(data["features"]), file=sys.stderr)

    skipped = 0
    features = []

    for f in data["features"]:
        tags = f["properties"]["@ns:com:here:xyz"]["tags"]

        not_found = 0

        for t in args.tags:
            if t not in tags:
                not_found += 1

        if not_found > 0:
            skipped += 1
            continue

        features.append(f)

    collection = geojson.FeatureCollection(features)
    print("kept %d features" % len(features), file=sys.stderr)
    geojson.dump(collection, sys.stdout)


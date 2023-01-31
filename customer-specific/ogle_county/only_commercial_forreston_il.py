#!/usr/bin/env python3

import sys
import geojson
import pprint

if __name__ == "__main__":
    data = geojson.load(sys.stdin)
    features = []
    not_std = not_il = not_commercial = 0

    for f in data["features"]:
        props = f["properties"]

        if "standardized_address" not in props:
            not_std += 1
            continue

        if props["state"] != "IL":
            not_il += 1
            continue

        rdi = props["standardized_address"]["metadata"].get("rdi", "n/a")

        if rdi != "Commercial":
            not_commercial += 1
            continue

        features.append(f)

    print("in=%d not_std=%d not_IL=%d not_comm=%d out=%d" %
          (len(data["features"]),
           not_std, not_il, not_commercial,
           len(features)),
        file=sys.stderr)
    collection = geojson.FeatureCollection(features)
    geojson.dump(collection, sys.stdout)



    

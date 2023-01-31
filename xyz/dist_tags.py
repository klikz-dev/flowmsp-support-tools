#!/usr/bin/env python3

import sys
import geojson

if __name__ == "__main__":
    data = geojson.load(sys.stdin)
    print("loaded %d features" % len(data["features"]))
    counts = {}

    for f in data["features"]:
        tags = f["properties"]["@ns:com:here:xyz"]["tags"]

        k = tuple([t for t in tags if t.startswith("street@") is False and t.startswith("district@") is False ])

        if k not in counts:
            counts[k] = 0

        counts[k] += 1

    for k in counts:
        print("%10d  %s" % (counts[k], k))

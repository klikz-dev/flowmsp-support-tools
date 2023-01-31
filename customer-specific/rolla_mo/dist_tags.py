#!/usr/bin/env python3

import sys
import geojson
import pprint

if __name__ == "__main__":
    data = geojson.load(sys.stdin)
    features = []
    counts = {}

    for f in data["features"]:
        props = f["properties"]

        # pprint.pprint(props)

        tags = [t for t in props["@ns:com:here:xyz"]["tags"] if t.startswith("street@") is False]
        k = tuple(tags)

        if k not in counts:
            counts[k] = 0

        counts[k] += 1

    for k in counts:
        print("%10d  %s" % (counts[k], k))

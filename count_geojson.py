#!/usr/bin/env python3

import sys
import geojson


if __name__ == "__main__":
    if len(sys.argv) > 1:
        for f in sys.argv[1:]:
            g = geojson.load(open(f))
            print("%s: features=%d" % (f, len(g["features"])))
    else:
        g = geojson.load(sys.stdin)
        print("%s: features=%d" % (sys.stdin.name, len(g["features"])))

        

      

#!/usr/bin/env python3

import sys
import json
import time
import csv


if __name__ == "__main__":
    dispatches = json.load(sys.stdin)

    hdr = [
        "tstamp",
        "textRaw",
        "text",
        "address",
        "lat",
        "lon"
    ]

    w = csv.writer(sys.stdout, lineterminator="\n")
    w.writerow(hdr)

    no_coord = 0

    for dnum, d in enumerate(dispatches):
        # convert sequence to timestamp
        tstamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(d["sequence"] / 1000))

        if "lonLat" not in d:
            no_coord += 1
            lat = lon = ""
        else:
            lat = d["lonLat"]["coordinates"][1]
            lon = d["lonLat"]["coordinates"][0]

        w.writerow([
            tstamp,
            d["textRaw"],
            d["text"],
            d["address"],
            lat,
            lon
        ])

    print("dispatches=%d no-coord=%d" % (len(dispatches), no_coord),
          file=sys.stderr)
    

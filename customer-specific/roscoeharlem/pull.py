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
        "lon",
        "distance"
    ]

    w = csv.writer(sys.stdout, lineterminator="\n")
    w.writerow(hdr)

    no_coord = beyond_50 = 0

    for dnum, d in enumerate(dispatches):
        # convert sequence to timestamp
        tstamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(d["sequence"] / 1000))

        if "lonLat" not in d:
            no_coord += 1
            lat = lon = ""
        else:
            lat = d["lonLat"]["coordinates"][1]
            lon = d["lonLat"]["coordinates"][0]

        raw_text = d.get("textRaw", "").replace("\r", " ")
        text = d.get("text", "").replace("\r", " ")
        distance = d.get("distance_in_miles", "")
        distance2 = d.get("distance_in_miles", -1)

        if distance2 != -1 and distance2 > 50.0:
            beyond_50 += 1

        w.writerow([
            tstamp,
            raw_text,
            text,
            d.get("address", ""),
            lat,
            lon,
            distance
        ])

    print("dispatches=%d no-coord=%d beyond_50=%d" %
          (len(dispatches), no_coord, beyond_50),
          file=sys.stderr)
    

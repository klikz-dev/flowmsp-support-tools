#!/usr/bin/env python3

import sys
import csv
import geojson

def make_header(feature):
    hdr = list(feature["properties"].keys())

    return hdr + ["LAT", "LON"]


if __name__ == "__main__":
    data = geojson.load(sys.stdin)

    print("name", data.get("name", "n/a"), file=sys.stderr)
    print("crs", data.get("crs", "n/a"), file=sys.stderr)
    print("type", data.get("type", "n/a"), file=sys.stderr)
    print("features=%d" % len(data["features"]), file=sys.stderr)

    w = csv.writer(sys.stdout, lineterminator="\n")

    hdr = make_header(data["features"][0])

    w.writerow(hdr)

    for feature in data["features"]:
        orec = []

        for h in hdr[:-2]:
            orec.append(feature["properties"].get(h, ""))

        try:
            orec.append(feature["geometry"]["coordinates"][1])
        except:
            orec.append("")

        try:
            orec.append(feature["geometry"]["coordinates"][0])
        except:
            orec.append("")

        w.writerow(orec)
    

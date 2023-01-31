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

    hdr = ["lat", "lon", "flow", "size",
           "address", "inservice", "notes",
           "dryhydrant", "outservicedate"]

    w = csv.DictWriter(sys.stdout, hdr, lineterminator="\n")
    w.writeheader()

    # hdr = make_header(data["features"][0])

    for feature in data["features"]:
        orec = {}
        orec["lat"] = feature["geometry"]["coordinates"][1]
        orec["lon"] = feature["geometry"]["coordinates"][0]
        orec["size"] = feature["properties"].get("MAIN_SIZE", "")
        orec["notes"] = feature["properties"].get("OBJECTID", "")

        w.writerow(orec)
    

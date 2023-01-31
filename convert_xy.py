#!/usr/bin/env python3

import sys
import csv
import argparse
import pyproj

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="check for broken links")
    parser.add_argument("coordinate_system")
    parser.add_argument("--limit", type=int)

    sys.stdin = open(sys.stdin.fileno(), errors="replace", encoding='utf8', buffering=1)

    args = parser.parse_args()
    r = csv.DictReader(sys.stdin)

    wgs84 = pyproj.Proj("+init=EPSG:4326")
    coord = pyproj.Proj("+init=%s" % args.coordinate_system)

    # hdr = ["OBJECTID", "X", "Y", "LAT", "LON"]
    hdr = r.fieldnames + ["LAT", "LON"]

    w = csv.DictWriter(sys.stdout, hdr, lineterminator="\n")
    w.writeheader()

    for recnum, rec in enumerate(r):
        if args.limit and recnum >= args.limit:
            break

        #orec = {}

        lon, lat = pyproj.transform(coord, wgs84, rec["X"], rec["Y"])

        rec["LAT"] = lat
        rec["LON"] = lon

        w.writerow(rec)


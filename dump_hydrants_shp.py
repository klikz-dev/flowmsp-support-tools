#!/usr/bin/env python3

import os
import sys
import csv
import argparse
import pprint
import shapefile
from pyproj import Proj, transform

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="read SHP file")
    parser.add_argument("shp_file")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    sf = shapefile.Reader(args.shp_file)
    print("%s contains %d records" % (args.shp_file, sf.numRecords), file=sys.stderr)

    # https://gis.stackexchange.com/questions/224526/convert-shapefile-from-projected-coordinates-using-python/224553
    inp = Proj(init='epsg:3436', preserve_units=True)
    outp = Proj(init="epsg:4326")

    hdr = sf.fields + [("LAT", "C", 1, 0), ("LON", "C", 1, 0)]
    hdr_fields = [h[0] for h in hdr]

    w = csv.writer(sys.stdout, lineterminator="\n")
    w.writerow(hdr_fields)
    inrecs = 0

    for sr in sf.shapeRecords():
        inrecs += 1

        if args.limit and inrecs > args.limit:
            break

        d = sr.record.as_dict()

        try:
            lon, lat = transform(inp, outp, sr.shape.points[0][0], sr.shape.points[0][1])
            d["LAT"] = lat
            d["LON"] = lon

        except:
            print(inrecs, "failed", sr.shape.points, d, file=sys.stderr)
            d["LAT"] = ""
            d["LON"] = ""

        if args.debug:
            print("------------------------------------------------------------")
            pprint.pprint(d)

        orec = [d.get(h,"") for h in hdr_fields]
        w.writerow(orec)

    print("inrecs=%d" % inrecs, file=sys.stderr)

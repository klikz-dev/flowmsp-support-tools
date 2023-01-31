#!/usr/bin/env python3

import os
import sys
import csv
import argparse
from pykml import parser as kmlparser

hdr = ["lat", "lon", "flow",
       "size", "address",
       "inservice", "notes",
       "dryhydrant",
       "outservicedate"]

def open_output_file(prefix, group):
    outfile = prefix % group

    fp = open(outfile, "w")
    w = csv.writer(fp, lineterminator="\n")
    w.writerow(hdr)

    print("opened %s" % fp.name, file=sys.stderr)
    return fp, w


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="read KML file")
    parser.add_argument("kml_file")
    parser.add_argument("--group-size", type=int, default=300)
    parser.add_argument("--prefix", default="hydrants_%03d.csv")
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()

    fp = open(args.kml_file)

    doc = kmlparser.parse(fp).getroot()

    inrecs = outrecs = rows = 0
    group = 1

    fp, w = open_output_file(args.prefix, group)

    for e in doc.Document.Folder.Placemark:
        inrecs += 1

        if args.limit and inrecs > args.limit:
            break

        id = e.ExtendedData.SchemaData.SimpleData
        lon,lat,_ = e.Point.coordinates.text.split(",")

        orec = [lat, lon,
                "0", # flow
                "",  # size
                "",  # address
                "",  # in-service
                id,  # notes
                "",  # dry hydrant
                "",  # out-service date
                ]

        if rows >= args.group_size:
            fp.close()
            rows = 0
            group += 1
            fp, w = open_output_file(args.prefix, group)

        w.writerow(orec)
        outrecs += 1
        rows += 1

    print("inrecs=%d outrecs=%d" % (inrecs, outrecs), file=sys.stderr)
    

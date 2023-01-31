#!/usr/bin/env python3

import os
import sys
import csv

r = csv.reader(sys.stdin)
w = csv.writer(sys.stdout, lineterminator="\n")

hdr = next(r)

ohdr = ["lat", "lon", "address"]

w.writerow(ohdr)

for rec in csv.reader(sys.stdin):
    fld = rec[2]

    if "[" in fld:
        i = fld.index("[")
        j = fld.index("]")

        latlon = fld[i+1:j]

        lon, lat = latlon.split(",")
    else:
        lat = lon = ""

    orec = [
        lat.strip(),
        lon.strip(),
        rec[3]
    ]

    w.writerow(orec)

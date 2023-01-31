#!/usr/bin/env python3

import sys
import csv

if __name__ == "__main__":
    r = csv.reader(sys.stdin)
    w = csv.writer(sys.stdout, lineterminator="\n")
    w.writerow(["lat", "lon", "src", "address"])

    for rec in r:
        if rec[0] == "lat":
            continue

        if len(rec[2]) > 10:
            src = "DB"
        else:
            src = "NEW"

        orec = [rec[0], rec[1], src, rec[-1]]
        w.writerow(orec)

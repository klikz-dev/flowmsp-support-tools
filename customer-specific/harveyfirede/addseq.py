#!/usr/bin/env python3

import sys
import csv

if __name__ == "__main__":
    r = csv.reader(sys.stdin)
    w = csv.writer(sys.stdout, lineterminator="\n")

    hdr = next(r)
    ohdr = ["recnum"] + hdr
    w.writerow(ohdr)

    for recnum, rec in enumerate(r):
        orec = ["%05d" % (recnum + 1)] + rec
        w.writerow(orec)


        

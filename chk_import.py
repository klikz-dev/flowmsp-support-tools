#!/usr/bin/env python3

import os
import sys
import csv

def find_location(rec, tbl):
    name = rec[1]
    addr = rec[2]

    found = 0

    for t in tbl:
        if t[5] == name and t[6] == addr:
            found += 1

    return found


def process_file(filename, tbl):
    fp = open(filename, errors="ignore")
    r = csv.reader(fp)
    hdr = None

    w = csv.writer(sys.stdout, lineterminator="\n")

    for recnum, rec in enumerate(r):
        if recnum == 0:
            hdr = rec
            continue

        n = find_location(rec, tbl)

        if n == 1:
            continue

        w.writerow([filename, recnum, n, rec[1], rec[2]])

    return


def load_locations(filename, tbl):
    fp = open(filename, errors="ignore")
    r = csv.reader(fp)
    hdr = next(r)

    for recnum, rec in enumerate(r):
        tbl.append(rec)

    return


if __name__ == "__main__":
    locations = []

    load_locations(sys.argv[1], locations)

    for filename in sys.argv[2:]:
        process_file(filename, locations)



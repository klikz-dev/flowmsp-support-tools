#!/usr/bin/env python3

import os
import sys
import csv

def find_location(rec, tbl):
    name = rec[5]
    addr = rec[6]

    found = 0

    for t in tbl:
        if t[5] == name and t[6] == addr:
            found += 1

    return found


def load_locations(filename, batch_num):
    tbl = []

    fp = open(filename, errors="ignore")
    r = csv.reader(fp)
    hdr = next(r)

    for rec in r:
        if rec[2] == batch_num:
            tbl.append(rec)

    print("%s: loaded %d records" % (batch_num, len(tbl)),
          file=sys.stderr)

    return tbl


if __name__ == "__main__":
    batch1 = load_locations(sys.argv[1], "1560793772080")
    batch2 = load_locations(sys.argv[1], "1560794030508")

    for r in batch1:
        n = find_location(r, batch2)

        if n == 0:
            print(r)

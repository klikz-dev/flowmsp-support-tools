#!/usr/bin/env python3

import os
import sys
import pprint
import argparse
import csv


def open_outfile(pattern, filenum, hdr):
    filename = pattern % filenum
    fp = open(filename, "w")

    w = csv.writer(fp, lineterminator="\n")
    w.writerow(hdr)

    return w, fp


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="split a csv file into smaller files")
    parser.add_argument("number", type=int)
    parser.add_argument("--pattern", default="outfile_%04d.csv")

    args = parser.parse_args()

    r = csv.reader(sys.stdin)

    hdr = next(r)
    count = 0
    filenum = 1

    w, fp = open_outfile(args.pattern, filenum, hdr)

    for rec in r:
        if count >= args.number:
            fp.close()
            filenum += 1
            w, fp = open_outfile(args.pattern, filenum, hdr)
            count = 0

        count += 1
        w.writerow(rec)

            

#!/usr/bin/env python3

import sys
import csv


def sort_func(a):
    return float(a[0]), float(a[1])


if __name__ == "__main__":
    r = csv.DictReader(sys.stdin)
    w = csv.writer(sys.stdout, lineterminator="\n")

    hdr = [
            "lat",
            "lon",
            "_id",
            "batchNo",
            "createdBy",
            "createdOn",
            "flow",
            "color",
            "modifiedBy",
            "modifiedOn",
            "size",
            "streetAddress"]
    w.writerow(hdr)

    recs = []

    for rec in r:
        addr = rec["streetAddress"].replace("\n", " ")

        orec = [
            rec["lat"],
            rec["lon"],
            rec["_id"],
            rec["batchNo"],
            rec["createdBy"],
            rec["createdOn"],
            rec["flow"],
            rec["color"],
            rec["modifiedBy"],
            rec["modifiedOn"],
            rec["size"],
            addr
        ]        

        recs.append(orec)

        #w.writerow(orec)

    for rec in sorted(recs, key=sort_func):
        w.writerow(rec)

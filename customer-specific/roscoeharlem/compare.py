#!/usr/bin/env python3

import sys
import json
import csv

def find_msg(msgs, id):
    for m in msgs:
        if m["textRaw"] == id:
            return m

    return


def get_info(m):
    keys = ["type", "address", "distance_in_miles", "lonLat"]

    return [m.get(k, "") for k in keys]


if __name__ == "__main__":
    prod = json.load(open(sys.argv[1]))
    test = json.load(open(sys.argv[2]))

    w = csv.writer(sys.stdout, lineterminator="\n")

    hdr = ["rawtext",
           "prod_type", "prod_address", "prod_miles", "prod_latlon",
           "test_type", "test_address", "test_miles", "test_latlon"]

    w.writerow(hdr)

    for i, t in enumerate(test):
        p = find_msg(prod, t["textRaw"])

        if not p:
            continue

        orec = [p.get("textRaw")]

        orec += get_info(p)
        orec += get_info(t)
        w.writerow(orec)

            

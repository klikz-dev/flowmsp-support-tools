#!/usr/bin/env python3

import sys
import csv
import argparse
import pprint


def load_geocoded(filename):
    tbl = {}
    r = csv.DictReader(open(filename), delimiter="|")

    return dict([[rec["recId"], rec] for rec in r if rec["SeqNumber"] == "1"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="batch geocoder for HERE.com")
    parser.add_argument("hydrant_locations")
    parser.add_argument("geocoded_file")

    args = parser.parse_args()

    geocoded_results = load_geocoded(args.geocoded_file)

    r = csv.DictReader(open(args.hydrant_locations))

    w = csv.writer(sys.stdout, lineterminator="\n")

    hdr = ["lat", "lon",
           "hydr_id", "Station", "district",
           "hydr_gpm",
           "make",
           "model",
           "number",
           "st_prefix",
           "street",
           "st_type",
           "st_suffix",
           "city",
           "state",
           "zip",
           "address"]

    w.writerow(hdr)

    for rec in r:
        rec_id = rec["hydr_id"]

        if not rec_id:
            continue

        geo = geocoded_results.get(rec_id, "n/a")

        orec = [geo["displayLatitude"],
                geo["displayLongitude"],
                rec["hydr_id"],
                rec["Station"],
                rec["district"],
                rec["hydr_gpm"],
                rec["make"],
                rec["model"],
                rec["number"],
                rec["st_prefix"],
                rec["street"],
                rec["st_type"],
                rec["st_suffix"],
                rec["city"],
                rec["state"],
                rec["zip"],
                geo["locationLabel"]]

        w.writerow(orec)


    
              

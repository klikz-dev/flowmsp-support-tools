#!/usr/bin/env python3

import sys
import csv
import argparse
from pymongo import MongoClient
import boto3
import botocore
import pprint
from geopy.distance import geodesic


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


def load_existing(db, slug):
    coll = "%s.Hydrant" % slug

    return [row for row in db[coll].find()]


def find_nearest(rec, existing, distance):
    this = (float(rec["lat"]), float(rec["lon"]))

    nearby = []

    for e in existing:
        lon, lat = e["lonLat"]["coordinates"]
        that = (lat, lon)
        r = geodesic(this, that)

        #print(r.feet, r.miles, r.meters)

        if r.feet <= distance:
            e["distance"] = r.feet
            nearby.append(e)

    if not nearby:
        return

    results = list(sorted(nearby, key=lambda h: h["distance"]))

    return results[0]

    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="check for broken links")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("customer_name", help="customer name, aka slug")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--multi-line", action="store_true")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--distance", type=int, default=75)
    parser.add_argument("--limit", type=int)

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")

    client = MongoClient(mongo_uri)
    db = client.FlowMSP

    existing = load_existing(db, args.customer_name)

    ohdr = ["lat", "lon", "flow", "size", "address", "inservice",
            "notes", "dryhydrant", "outservicedate"]

    r = csv.DictReader(sys.stdin)
    w = csv.DictWriter(sys.stdout, ohdr, lineterminator="\n")
    w.writeheader()
    inrecs = matches = 0

    for recnum, rec in enumerate(r):
        inrecs += 1
        
        orec = {}
        orec["lat"] = rec["lat"]
        orec["lon"] = rec["lon"]
        orec["flow"] = rec["hydr_gpm"]
        orec["address"] = rec["address"]
        orec["notes"] = rec["hydr_id"]

        nearby = find_nearest(rec, existing, args.distance)

        # if we find a nearby hydrant, use its LAT/LON and address
        if nearby:
            matches += 1
            print("------------------------------------------------------------", file=sys.stderr)
            print("Match", recnum, orec["address"], orec["lat"], orec["lon"], file=sys.stderr)
            orec["lat"] = nearby["lonLat"]["coordinates"][1]
            orec["lon"] = nearby["lonLat"]["coordinates"][0]
            orec["address"] = nearby["streetAddress"].replace("\n", " ")
            print("DB", orec["address"], orec["lat"], orec["lon"], nearby["distance"], file=sys.stderr)

        w.writerow(orec)

    print("inrecs=%d matches=%d" % (inrecs, matches), file=sys.stderr)


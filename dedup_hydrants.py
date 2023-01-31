#!/usr/bin/env python3

import os
import sys
import pprint
import argparse
import json
import csv
from pymongo import MongoClient
import boto3
import botocore
import geojson


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


def get_db(profile):
    session = boto3.session.Session(profile_name=profile)
    mongo_uri = get_parm(session, "mongo_uri")

    client = MongoClient(mongo_uri)
    db = client.FlowMSP

    return db, client


def print_hydrant(label, h):
    if "createdOn" in h:
        tstamp = h["createdOn"].strftime("%Y-%m-%d %H:%M:%S")
    else:
        tstamp = ""

    print(label,
          [h["_id"],
          h["customerSlug"],
          tstamp,
          "%s,%s" % (h["lonLat"]["coordinates"][1], h["lonLat"]["coordinates"][0]),
          h.get("flow", ""),
          h.get("streetAddress", ""),
           h.get("notes", "")])

    return


def find_nearby_hydrants(db, dst, row, distance_in_meters):
    # "_id": { "$ne": row["_id"]},
    q = {
        "lonLat": {'$near': {'$geometry': row["lonLat"], '$maxDistance': distance_in_meters}}
        }

    return [h for h in dst.find(q)]
    

def get_customer(db, slug):
    return db.Customer.find_one({"slug": slug})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="copy hydrants")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("slug")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--distance-in-feet", type=int, default=100)

    args = parser.parse_args()

    db, client = get_db(args.profile)
    rows = dups = 0
    distance_in_meters = args.distance_in_feet * .3048

    coll = "%s.Hydrant" % args.slug

    for rownum, row in enumerate(db[coll].find()):
        if args.limit and rownum >= args.limit:
            break

        rows += 1

        dup_hydrants = find_nearby_hydrants(db, db[coll], row, distance_in_meters)

        if len(dup_hydrants) > 1:
            dups += 1

            print("-----------------------------------------------------------")
            for h in dup_hydrants:
                if h["_id"] == row["_id"]:
                    label = "row"
                else:
                    label = "dup"
                    
                #pprint.pprint(h)
                print_hydrant(label, h)

            print()

    print("%s: rows=%d dups=%d" % (args.slug, rows, dups),
          file=sys.stderr)


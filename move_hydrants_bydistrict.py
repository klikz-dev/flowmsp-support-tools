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


def get_collections(db, dst_collection, src):
    if src:
        for s in src:
            c = "%s.Hydrant" % s

            if c in db.list_collection_names():
                yield c

    else:
        for c in db.list_collection_names():
            if c == dst_collection:
                continue
    
            if "dhware,inc" in c:
                continue
    
            if c.endswith(".Hydrant"):
                yield c

    return


def get_db(profile):
    session = boto3.session.Session(profile_name=profile)
    mongo_uri = get_parm(session, "mongo_uri")

    client = MongoClient(mongo_uri)
    db = client.FlowMSP

    return db, client


def print_hydrant(label, h):
    if "createdOn" in h:
        tstamp = h["createdOn"].strftime("%Y-%m-%d %H:%M:%S"),
    else:
        tstamp = ""

    print(label,
          h["_id"],
          h["customerSlug"],
          tstamp,
          h["lonLat"],
          h.get("flow", ""),
          h.get("streetAddress", ""),
          h.get("notes", ""))

    return


def find_nearby_hydrants(db, dst, row):
    q = {'lonLat': {'$near': {'$geometry': row["lonLat"], '$maxDistance': 30.48}}}

    return [h for h in dst.find(q)]
    

def get_customer(db, slug):
    return db.Customer.find_one({"slug": slug})


def move_hydrant(client, src, dst, row, customer, dup):
    with client.start_session() as s:
        s.start_transaction()

        # remove hydrant from src
        r = src.delete_one({"_id": row["_id"]})

        if not dup:
            print("------------------------- BEFORE -----------------------------------")
            pprint.pprint(row)

            row["customerId"] = customer["_id"]
            row["customerSlug"] = customer["slug"]

            print("------------------------- AFTER  -----------------------------------")
            pprint.pprint(row)

            r = dst.insert_one(row)

        s.commit_transaction()

    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="copy hydrants")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("fire_district")
    parser.add_argument("dst")
    parser.add_argument("src", nargs="*")
    parser.add_argument("--index", type=int, default=0)
    parser.add_argument("--list", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", action="store_true")

    args = parser.parse_args()

    data = geojson.load(open(args.fire_district))

    if args.list:
        for i, f in enumerate(data["features"]):
            print(i, f["properties"]["NAME"])

        raise SystemExit(0)

    db, client = get_db(args.profile)
    rows = 0

    district = data["features"][args.index]
    customer = get_customer(db, args.dst)

    pprint.pprint(district["properties"], stream=sys.stderr)

    dst = "%s.Hydrant" % args.dst

    for src in get_collections(db, dst, args.src):
        q = {'lonLat': {'$geoWithin': {'$geometry': district["geometry"]}}}

        rows = dups = 0

        if args.verbose:
            print(src)

        for row in db[src].find(q):
            rows += 1

            print("----------------------------- row ------------------------------")
            pprint.pprint(row)

            dup_hydrants = find_nearby_hydrants(db, db[dst], row)

            if args.dry_run is False:
                move_hydrant(client, db[src], db[dst], row, customer, bool(dup_hydrants))

            print("----------------------------- dups ------------------------------")
            for h in dup_hydrants:
                print_hydrant("dup", h)

            if dup_hydrants:
                dups += 1

        if rows:
            print("%s rows=%d dups=%d" % (src, rows, dups), file=sys.stderr)

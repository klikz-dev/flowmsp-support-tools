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
            c = "%s.Location" % s

            if c in db.list_collection_names():
                yield c

    else:
        for c in db.list_collection_names():
            if c == dst_collection:
                continue
    
            if "dhware,inc" in c:
                continue
    
            if c.endswith(".Location"):
                yield c

    return


def get_db(profile):
    session = boto3.session.Session(profile_name=profile)
    mongo_uri = get_parm(session, "mongo_uri")

    client = MongoClient(mongo_uri)
    db = client.FlowMSP

    return db, client


def find_duplicate_location(db, dst, row):
    # q = {"geoOutline": {'$near': {'$geometry': row["geoOutline"], '$maxDistance': 30.48}}}
    q = {"geoOutline": {'$geoIntersects': {'$geometry': row["geoOutline"]}}}

    return [h for h in dst.find(q)]
    

def get_customer(db, slug):
    return db.Customer.find_one({"slug": slug})


def move_location(client, src, dst, row, customer, dup):
    with client.start_session() as s:
        s.start_transaction()

        # remove from src
        r = src.delete_one({"_id": row["_id"]})

        if not dup:
            print("------------------------- BEFORE -----------------------------------")
            pprint.pprint(row)

            row["customerId"] = customer["_id"]
            row["customerSlug"] = customer["slug"]
            row.pop("hydrants")

            print("------------------------- AFTER  -----------------------------------")
            pprint.pprint(row)

            r = dst.insert_one(row)

        s.commit_transaction()

    return


def user_modified(row):
    if len(row.get("images", [])) > 0:
        return True

    if row.get("name", "") != "TBD":
        return True

    if row.get("createdBy", "") != "load_locations2.py":
        return True

    if row.get("building", {}).get("lastReviewedBy", "") != "load_locations2.py":
        return True

    # created manually, leave alone
    if row.get("batchNo", 0) == 0:
        return True

    #if row.get("createdOn", "") != row.get("modifiedOn", ""):
    #    return True

    return False


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

    district = data["features"][args.index]
    customer = get_customer(db, args.dst)

    pprint.pprint(district["properties"], stream=sys.stderr)

    print("------------------------------------------------------------")

    dst = "%s.Location" % args.dst

    for src in get_collections(db, dst, args.src):
        q = {"geoOutline": {'$geoWithin': {'$geometry': district["geometry"]}}}

        rows = dups = user_data = 0

        #print("------------------------------------------------------------")
        #print(src)
        #print("------------------------------------------------------------")

        for row in db[src].find(q):
            rows += 1

            print(row["name"], row["createdBy"], row.get("address", {}).get("address1", ""))

            if user_modified(row):
                user_data += 1
                continue
                
            print("----------------------------- row ------------------------------")
            #print(row["name"], row["createdBy"], row.get("address", {}).get("address1", ""))
            pprint.pprint(row)

            dup_locations = find_duplicate_location(db, db[dst], row)

            if args.dry_run is False:
                move_location(client, db[src], db[dst], row, customer, bool(dup_locations))

            if dup_locations:
                dups += 1

                print("----------------------------- dups ------------------------------")
                for h in dup_locations:
                    #print(h["name"], h["createdBy"])
                    pprint.pprint(h)

        if rows:
            print("%s rows=%d user_data=%d dups=%d" %
                  (src, rows, user_data, dups),
                  file=sys.stderr)

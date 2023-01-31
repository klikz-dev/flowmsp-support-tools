#!/usr/bin/env python3

import os
import sys
import uuid
import pprint
import time
import json
import argparse
import csv
import datetime
from pymongo import MongoClient
import boto3
import botocore
import shapely
import shapely.geometry
import requests
from geopy import Here
from geopy import GoogleV3


def geocode(row, mapkey):
    # g = Here("WaGiLAKzJQzySaXFJm9o", "bO4lKri2pJuiCxnRuD0-KQ")
    g = GoogleV3(mapkey)

    # pprint.pprint(row)

    address = ", ".join([
        row["address"]["address1"],
        row["address"]["city"],
        row["address"]["state"],
        row["address"]["zip"]
        ])

    r = g.geocode(address, timeout=5)

    return r.latitude, r.longitude


def get_outline(db, lat, lon):
    query = {
        "geometry": {
            "$geoIntersects": {
                "$geometry": {
                    "type": "Point",
                    "coordinates": [ lon, lat ]
                }
            }
        }
    }

    row = db.ms_geodata.find_one(query)

    if row:
        return row
    else:
        return {
            "geometry": {"coordinates": [[[lon, lat],
                                          [lon - .00005, lat],
                                          [lon - .00005, lat - .00005],
                                          [lon, lat - .00005],
                                          [lon, lat]]],
                         "type": "Polygon"}
        }


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


def get_customer(db, slug):
    return db.Customer.find_one({"slug": slug})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="load RMS data")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("customer_name", help="customer name, aka slug")
    parser.add_argument("csv_file")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--truncate", action="store_true")
    parser.add_argument("--batch-size", type=int, default=250)

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")
    map_key = get_parm(session, "GOOGLE_MAP_API_KEY")

    PGM = os.path.basename(sys.argv[0])

    client = MongoClient(mongo_uri)
    db = client.FlowMSP

    batchNo = time.time_ns()
    collection = "%s.Location" % args.customer_name
    customer = get_customer(db, args.customer_name)
    print("customer_id=%s" % customer["_id"])

    rows = skipped = 0

    if args.truncate:
        result = db[collection].delete_many({})
        print("deleted %d rows" % result.deleted_count)

    batch = []

    fp = open(args.csv_file, errors="ignore")
    r = csv.DictReader(fp)

    for rownum, rec in enumerate(r):
        if args.limit and rownum >= args.limit:
            break

        rows += 1

        if args.verbose:
            if rows % 25 == 0:
                print("rows=%d" % rows)

        now = datetime.datetime.now()

        addr1 = "{number} {st_prefix} {street} {st_suffix}".format(
            number=rec["number"],
            st_prefix=rec["st_prefix"],
            street=rec["street"],
            st_suffix=rec["st_suffix"])

        row = {
            "_id": str(uuid.uuid4()),
            "address": {
                "address1": addr1,
                "address2": rec["addr_2"],
                "city": rec["city"],
                "state": rec["state"],
                "zip": rec["zip"],
            },
            "batchNo": batchNo,
            "createdOn": now,
            "createdBy": PGM,
            "customerSlug": args.customer_name,
            "customerId": customer["_id"],
            "name": rec["occ_name"],
            "images": [],
            "hydrants": [],
            "building": {
                "normalPopulation": "",
                "lastReviewedBy": PGM,
                "lastReviewedOn": now.strftime("%m-%d-%Y %H.%M.%S"),
                "originalPrePlan": now.strftime("%m-%d-%Y %H.%M.%S"),
            },
            "requiredFlow": 0,
            "roofArea": 0,
            "geoOutline": [],
        }

        lat, lon = geocode(row, map_key)

        outline = get_outline(db, lat, lon)

        if not outline or "geometry" not in outline:
            print(rec, "building geometry not found", file=sys.stderr)
        else:
            row["geoOutline"] = outline["geometry"]

        if args.debug:
            print("------------------------------------------------------------")
            print("rownum=%d" % rownum)
            print("feature")
            pprint.pprint(feature)
            print()
            print("row")
            pprint.pprint(row)

        batch.append(row)

        if len(batch) == args.batch_size:
            db[collection].insert_many(batch)
            batch = []

    if batch:
        db[collection].insert_many(batch)

    print("rows=%d skipped=%d" % (rows, skipped), file=sys.stderr)

#!/usr/bin/env python3

import os
import sys
import uuid
import pprint
import time
import json
import argparse
import csv
import random
import datetime
from pymongo import MongoClient
import boto3
import botocore
import shapely
import shapely.geometry
import requests
import geojson


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


def get_customer(db, slug):
    row = db.Customer.find_one({"slug": slug})

    return row


def load_locations(db, collection):
    locations = []

    for row in db[collection].find({}):
        row["shape"] = shapely.geometry.asShape(row["geoOutline"])
        locations.append(row)

    print("loaded %d existing locations" % len(locations),
          file=sys.stderr)

    return locations


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

    return db.ms_geodata.find_one(query)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="load locations from geojson")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("customer_name", help="customer name, aka slug")
    parser.add_argument("geojson_file")
    parser.add_argument("geofence_file")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--truncate", action="store_true")
    parser.add_argument("--batch-size", type=int, default=250)
    parser.add_argument("--shuffle", action="store_true", help="randomly shuffle locations before loading")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--business-name", default="ORG_NAME")
    parser.add_argument("--geofence-index", type=int, default=0)
    parser.add_argument("--geofence-list", action="store_true")
    parser.add_argument("--address1", default="ADDR")
    parser.add_argument("--address2", default="ADDR2")
    parser.add_argument("--city", default="CITY")
    parser.add_argument("--state", default="IL")
    parser.add_argument("--zip", default="ZIP")
    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")


    PGM = os.path.basename(sys.argv[0])

    client = MongoClient(mongo_uri)
    db = client.FlowMSP

    data = geojson.load(open(args.geojson_file))
    print("features=%d" % len(data["features"]), file=sys.stderr)

    geofence = geojson.load(open(args.geofence_file))

    if args.geofence_list:
        for i, f in enumerate(geofence["features"]):
            print(i, f["properties"]["ORG_NAME"])

        raise SystemExit(0)

    #gf = geofence["features"][args.geofence_index]["geometry"]
    gf = shapely.geometry.asShape(geofence["features"][args.geofence_index]["geometry"])
    print(geofence["features"][args.geofence_index]["properties"], file=sys.stderr)

    if args.shuffle:
        g = random.Random()
        g.shuffle(data["features"])

    if "time_ns" in dir(time):
        batchNo = time.time_ns()
    else:
        batchNo = int(time.time())

    collection = "%s.Location" % args.customer_name
    customer = get_customer(db, args.customer_name)
    customer_id = customer["_id"]
    print("customer_id=%s" % customer_id, file=sys.stderr)

    rows = skipped = 0

    if args.truncate:
        result = db[collection].delete_many({})
        print("deleted %d rows" % result.deleted_count, file=sys.stderr)

    batch = []
    existing_locations = load_locations(db, collection)

    for rownum, feature in enumerate(data["features"]):
        if args.limit and rows >= args.limit:
            break

        #if args.debug:
        #    print("rownum", rownum, file=sys.stderr)
        #    print(feature["properties"], file=sys.stderr)

        if args.verbose:
            if rownum % 25 == 0:
                print("rownum=%d rows=%d skipped=%d" % (rownum, rows, skipped), file=sys.stderr)

        now = datetime.datetime.now()

        props = feature["properties"]

        lat = feature["geometry"]["coordinates"][1]
        lon = feature["geometry"]["coordinates"][0]

        p = shapely.geometry.Point(lon, lat)

        if gf.contains(p) is False:
            skipped += 1
            continue

        outline = get_outline(db, lat, lon)

        pprint.pprint(props)

        addr1 = " ".join([props.get(a, "") or "" for a in args.address1.split(",")])

        row = {
            "_id": str(uuid.uuid4()),
            "address": {
                "address1": props.get(args.address1, "") or "",
                "address2": props.get(args.address2, "") or "",
                "city": props.get(args.city, "") or "",
                "state": args.state,
                "zip": props.get(args.zip, "") or "",
            },
            "batchNo": batchNo,
            "createdOn": now,
            "createdBy": PGM,
            "customerSlug": args.customer_name,
            "customerId": customer_id,
            "name": props.get(args.business_name, "") or "",
            "images": [],
            "hydrants": [],
            "building": {
                "normalPopulation": "",
                "lastReviewedBy": PGM,
                "lastReviewedOn": now.strftime("%m-%d-%Y %H.%M.%S"),
                "originalPrePlan": now.strftime("%m-%d-%Y %H.%M.%S"),
            },
            "roofArea": 0,
        }

        if outline and "geometry" in outline:
            row["geoOutline"] = outline["geometry"]
        else:
            print("no polygon found")
            print(feature)
            skipped += 1
            continue

        rows += 1

        if args.debug:
            print("------------------------------------------------------------")
            print("rownum=%d" % rownum)
            print("feature")
            pprint.pprint(feature)
            print()
            print("row")
            pprint.pprint(row)

        s = shapely.geometry.asShape(feature["geometry"])
        dups = 0

        for e in existing_locations:
            if s.intersects(e["shape"]):
                print("polygon overlaps", e["address"])
                dups += 1

        if dups:
            print("duplicate", row["address"])
            continue

        batch.append(row)

        if len(batch) == args.batch_size:
            if args.dry_run is False:
                db[collection].insert_many(batch)
            batch = []

    if batch:
        if args.dry_run is False:
            db[collection].insert_many(batch)

    print("rows=%d skipped=%d" % (rows, skipped), file=sys.stderr)

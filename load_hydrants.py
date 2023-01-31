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
import geojson


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


def get_customer_id(db, slug):
    row = db.Customer.find_one({"slug": slug})

    return row["_id"]


base_url = "https://maps.googleapis.com/maps/api/geocode/json"

def reverse_geocode(polygon, map_key):
    polygon = shapely.geometry.asShape(polygon)
    p = polygon.centroid
    url = "{url}?latlng={lat},{lon}&key={key}".format(
        url=base_url,
        lat=p.y,
        lon=p.x,
        key=map_key)

    r = requests.get(url)

    if not r.ok:
        return

    jdata = r.json()

    # this will need some work
    # for now, return the first street address
    for r in jdata["results"]:
        #pprint.pprint(r)

        if "street_address" in r["types"]:
            return get_address(r)

    return


def force_coordinates_to_floats(feature):
    coords = feature["geometry"]["coordinates"][0]

    for i, c in enumerate(coords):
        for j, v in enumerate(c):
            if type(v) is int:
                coords[i][j] = float(coords[i][j])

    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="check for broken links")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("customer_name", help="customer name, aka slug")
    parser.add_argument("geojson_file")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--multi-line", action="store_true")
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
    collection = "%s.Hydrant" % args.customer_name
    customer_id = get_customer_id(db, args.customer_name)
    print("customer_id=%s" % customer_id)

    rows = skipped = 0

    if args.truncate:
        result = db[collection].delete_many({})
        print("deleted %d rows" % result.deleted_count)

    batch = []

    r = csv.DictReader(open(args.geojson_file))

    for rownum, rec in enumerate(r):
        rows += 1

        if args.limit and rows >= args.limit:
            break

        if args.verbose:
            if rows % 25 == 0:
                print("rows=%d" % rows)

        now = datetime.datetime.now()

        try:
            lat = float(rec["lat"])
            lon = float(rec["lon"])
        except:
            if args.verbose:
                print("recnum", rownum)
                pprint.pprint(rec)
            skipped += 1
            continue

        row = {
            "_id": str(uuid.uuid4()),
            "batchNo": batchNo,
            "createdOn": now,
            "createdBy": PGM,
            "customerId": customer_id,
            "customerSlug": args.customer_name,
            "dryHydrant": False,
            "flowRange": {"label": "Unknown", "pinColor": "YELLOW"},
            "inService": True,
            "lonLat": {"type": "Point", "coordinates": [lon, lat]},
        }

        batch.append(row)

        if len(batch) == args.batch_size:
            db[collection].insert_many(batch)
            batch = []

    if batch:
        db[collection].insert_many(batch)

    print("rows=%d skipped=%d" % (rows, skipped), file=sys.stderr)

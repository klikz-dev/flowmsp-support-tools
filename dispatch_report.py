#!/usr/bin/env python3

import os
import sys
import pprint
import argparse
import csv
from pymongo import MongoClient
import boto3
import botocore
from geopy.distance import geodesic

def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


def process_dispatches(db, collection, c):
    rows = geocoded = 0
    slug = collection.split(".")[0]
    clat = c["address"]["latLon"]["coordinates"][1]
    clon = c["address"]["latLon"]["coordinates"][0]
    station = (clat, clon)
    within_5 = within_10 = within_25 = within_50 = beyond_50 = 0

    for row in db[collection].find():
        rows += 1

        if "lonLat" in row:
            dlat = row["lonLat"]["coordinates"][1]
            dlon = row["lonLat"]["coordinates"][0]
            dispatch = (dlat, dlon)
            miles = geodesic(station, dispatch).miles

            # print(station, dispatch, miles)
            if miles <= 5:
                within_5 += 1
            elif miles <= 10:
                within_10 += 1
            elif miles <= 25:
                within_25 += 1
            elif miles <= 50:
                within_50 += 1
            else:
                beyond_50 += 1
                
            geocoded += 1

    if rows > 0:
        pct = (geocoded * 100) / rows
    else:
        pct = 0

    return [slug, rows, geocoded, within_5, within_10, within_25, within_50, beyond_50]


def get_customers(db):
    return dict([[row["slug"], row] for row in db.Customer.find()])
        

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="set dispatcher fields")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("--limit", type=int)

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")
    client = MongoClient(mongo_uri)
    db = client.FlowMSP
    counts = {}
    customers = get_customers(db)

    hdr = ["slug", "rows", "geocoded", "within_5", "within_10", "within_25", "within_50", "beyond_50"]
    w = csv.writer(sys.stdout, lineterminator="\n")
    w.writerow(hdr)
    processed = 0

    for row in db.list_collections():
        if row["name"].endswith(".MsgReceiver"):
            if args.limit and processed >= args.limit:
                break

            processed += 1
            s, _ = row["name"].rsplit(".", 1)

            if s in customers:
                result = process_dispatches(db, row["name"], customers[s])
            else:
                print("%s: not found in customers" % s, file=sys.stderr)

            w.writerow(result)

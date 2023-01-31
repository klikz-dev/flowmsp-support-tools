#!/usr/bin/env python3

import os
import sys
import pprint
import argparse
import csv
import time
import json
from pymongo import MongoClient
import boto3
import botocore
import tabulate
from geopy.distance import geodesic
import logging
from utils import get_db

logging.basicConfig()
logger = logging.getLogger("dispatches")
logger.setLevel(logging.INFO)


def get_config(db, slug):
    row = db["Customer"].find_one({"slug": slug})

    return row


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="check for broken links")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("slug")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--text")
    parser.add_argument("--truncate", action="store_true")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--add-timestamp", action="store_true")
    parser.add_argument("--add-distance", action="store_true")
    parser.add_argument("--start-time", help="yyyy-mm-dd format")

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    client, db = get_db(session)

    coll = "%s.MsgReceiver" % args.slug

    if args.truncate:
        result = db[coll].delete_many({})
        print("deleted %d rows" % result.deleted_count)
        raise SystemExit(0)

    cust = get_config(db, args.slug)

    if coll not in db.list_collection_names():
        print("%s: %s not found" % (args.slug, coll),
              file=sys.stderr)
        raise SystemExit(0)

    results = []
    clat = cust["address"]["latLon"]["coordinates"][1]
    clon = cust["address"]["latLon"]["coordinates"][0]
    station = (clat, clon)

    query = {}

    if args.start_time:
        t = time.strptime(args.start_time, "%Y-%m-%d")
        t2 = int(time.mktime(t)) * 1000
        query["sequence"] = {"$gte": t2}

    for rownum, row in enumerate(db[coll].find(query)):
        if args.limit and rownum >= args.limit:
            break

        if args.add_distance:
            if "lonLat" in row:
                dlat = row["lonLat"]["coordinates"][1]
                dlon = row["lonLat"]["coordinates"][0]
                dispatch = (dlat, dlon)
                row["distance_in_miles"] = geodesic(station, dispatch).miles

        if args.add_timestamp:
            tstamp = time.localtime(row["sequence"]/1000)
            row["timestamp"] = time.strftime("%Y-%m-%d %H:%M:%S", tstamp)

        if args.text:
            if args.text.lower() in row["text"].lower():
                results.append(row)
        else:
            results.append(row)

    json.dump(results, sys.stdout, indent=4)
    logger.info("read %d dispatches" % len(results))

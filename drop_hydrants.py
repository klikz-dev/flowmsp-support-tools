#!/usr/bin/env python3

import os
import sys
import pprint
import argparse
import json
import csv
import logging
from pymongo import MongoClient
import boto3
import botocore
import geojson


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="drop hydrants")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("slug")
    parser.add_argument("fire_district")
    parser.add_argument("--update", action="store_true")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--index", type=int, default=0)

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")

    client = MongoClient(mongo_uri)
    db = client.FlowMSP

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    coll = "%s.Hydrant" % args.slug

    gf = geojson.load(open(args.fire_district))
    district = gf["features"][args.index]

    q = {'lonLat': { "$not": {'$geoWithin': {'$geometry': district["geometry"]}}}}

    logging.debug(q)
    rows = deleted = 0

    for rownum, row in enumerate(db[coll].find(q)):
        rows += 1
        logging.info(row)

        if args.update:
            r = db[coll].delete_one({"_id": row["_id"]})
            deleted += r.deleted_count

    logging.info("rows=%d deleted=%d" % (rows, deleted))

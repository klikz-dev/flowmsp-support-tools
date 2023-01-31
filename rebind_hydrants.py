#!/usr/bin/env python3

import os
import sys
import pprint
import argparse
from pymongo import MongoClient
import boto3
import botocore
import datetime
import logging
import time
import uuid


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


def load_hydrants(db, slug):
    collection = "%s.Hydrant" % slug
    hydrants = {}

    for row in db[collection].find():
        hydrants[row["_id"]] = row

    logging.info("loaded %d hydrants" % len(hydrants))

    return hydrants


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="remove hydrants from preplan")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("slug")
    parser.add_argument("batch_number", type=int)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--save-collection", default="hydrant_mappings")
    parser.add_argument("--update", action="store_true")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--verbose", action="store_true")

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")
    client = MongoClient(mongo_uri)
    db = client.FlowMSP
    coll = "%s.Location" % args.slug
    rows = updated = 0

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    logging.debug("coll=%s" % coll)

    q = {"slug": args.slug, "batchNo": args.batch_number}

    hydrants = load_hydrants(db, args.slug)

    for rownum, row in enumerate(db[args.save_collection].find(q)):
        if args.limit and rownum >= args.limit:
            break

        rows += 1

        logging.debug("-------------------- rownum=%d --------------------" % rownum)
        logging.debug(row)

        loc = db[coll].find_one({"_id": row["location_id"]})

        loc["hydrants"] = []

        # delete hydrants that no longer exist
        for h in row["hydrants"]:
            if h not in hydrants:
                logging.info("%s no longer exists" % h)
            else:
                loc["hydrants"].append(h)

        # if at least one of the hydrants still exists, update row
        if loc["hydrants"] and args.update:
            r = db[coll].replace_one({"_id": loc["_id"]}, loc)
            updated += 1

    logging.info("rows=%d updated=%d" % (rows, updated))



#!/usr/bin/env python3

import os
import sys
import pprint
import argparse
import logging
from pymongo import MongoClient
import boto3
import botocore
import geojson


logging.basicConfig()
logger = logging.getLogger("pull_location")

def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="check for broken links")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("slug")
    parser.add_argument("names", nargs="+")
    parser.add_argument("--debug", action="store_true")

    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")
    client = MongoClient(mongo_uri)

    db = client.FlowMSP

    coll = "%s.Location" % args.slug
    query = {}

    features = []
    rows = found = 0

    for n in args.names:
        rows += 1
        row = db[coll].find_one({"name": n})

        if row:
            found += 1

            orec = {
                "type": "Feature",
                "properties": {
                    "name": row["name"],
                },
                "geometry": row["geoOutline"],
            }

            logger.info("name=%s" % row["name"])
            features.append(orec)

    logger.info("rows=%d found=%d" % (rows, found))
    collection = geojson.FeatureCollection(features)
    geojson.dump(collection, sys.stdout)



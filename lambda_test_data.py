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
import csv
import random
import json
import re


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


def get_collections(db):
    return [c for c in db.list_collection_names() if c.endswith(".Location")]


def get_locations(db, args):
    if args.slug:
        collections = ["%s.Location" % args.slug]
    else:
        collections = get_collections(db)

    # shuffle
    g = random.Random()
    g.shuffle(collections)

    q = {}
    rows = tables = 0

    for c in collections:
        tables += 1

        logging.info("collection=%s" % c)

        for row in db[c].find(q):
            rows += 1
            
            # no images
            if len(row.get("images", [])) == 0:
                continue
            
            # no annotations
            for img in row["images"]:
                if "annotationMetadata" not in img:
                    continue

                if args.text and args.text not in img["annotationMetadata"]:
                    continue

                if args.angle:
                    if "angle" not in img["annotationMetadata"]:
                        continue

                    jdata = json.loads(img["annotationMetadata"])

                    if jdata.get("backgroundImage", {}).get("angle", 0) == 0:
                        continue

                    # set to something other than zero
                    if not re.search(r"angle\":[1-9]", img["annotationMetadata"]):
                        continue

                yield row["customerSlug"], row["_id"], img

    logging.info("tables=%d rows=%d" % (tables, rows))

    return
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="generate test data for lambda functions")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("function", choices=["createdAnnotatedImage", "createAnnotatedThumbnail"])
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--slug")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--text")
    parser.add_argument("--angle", action="store_true")

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")
    client = MongoClient(mongo_uri)
    db = client.FlowMSP
    rows = updated = 0

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    results = []

    for rownum, row in enumerate(get_locations(db, args)):
        if args.limit and rownum >= args.limit:
            break

        slug, loc_id, img = row

        orec = {
            "slug": slug,
            "locationId": loc_id,
            "imageId": img["_id"]
        }
        
        if args.function == "createdAnnotatedImage":
            orec["annotationMetadataJSON"] = img["annotationMetadata"]

        results.append(orec)
        rows += 1

    json.dump(results, sys.stdout, indent=4)

    logging.info("rows=%d" % rows)



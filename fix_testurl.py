#!/usr/bin/env python3

import os
import sys
import argparse
import boto3
import json
import pprint
from pymongo import MongoClient

def get_customers(db):
    return [row["slug"] for row in db.Customer.find()]
        

def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


def fix_slug(db, slug, from_url, to_url, verbose):
    locations = images = corrected = 0

    for row in db["%s.Location" % slug].find():
        locations += 1
        needs_update = False

        if "images" not in row:
            continue

        for i, image in enumerate(row["images"]):
            images += 1

            for attr in "annotationMetadata", "annotationSVG":
                if attr in image and from_url in image[attr]:
                    row["images"][i][attr] = image[attr].replace(
                        from_url, to_url)
                    needs_update = True
                                   
                    if verbose:
                        print(row["address"]["address1"], i, attr, file=sys.stderr)

        if needs_update:
            id = row["_id"]
            db["%s.Location" % slug].replace_one({"_id": row["_id"]}, row)
            corrected += 1


    print("%s: locations=%d images=%d corrected=%d" % (slug, locations, images, corrected),
          file=sys.stderr)

    return

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="fix test URLs in annotations")
    parser.add_argument("profile", choices=["flowmsp-dev", "flowmsp-prod"])
    parser.add_argument("slugs", nargs="+")
    parser.add_argument("--from-url", default="https://test.flowmsp.com")
    parser.add_argument("--to-url", default="https://app.flowmsp.com")
    parser.add_argument("--verbose", action="store_true")

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")

    client = MongoClient(mongo_uri)

    db = client.FlowMSP

    for s in args.slugs:
        fix_slug(db, s, args.from_url, args.to_url, args.verbose)


#!/usr/bin/env python3

import os
import sys
import pprint
import argparse
from pymongo import MongoClient
import boto3
import botocore


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="check for broken links")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("customer_name", help="customer name, aka slug")
    parser.add_argument("location_id")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--multi-line", action="store_true")
    parser.add_argument("--debug", action="store_true")

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")

    client = MongoClient(mongo_uri)

    db = client.FlowMSP

    collection = "%s.Location" % args.customer_name
    location = db[collection].find_one({"_id": args.location_id})

    if args.multi_line:
        pprint.pprint(location)
    else:
        print(location["_id"], location["name"], location.get("address", {}).get("address1"))

        for i, img in enumerate(location.get("images", [])):
            if args.verbose:
                print("------------------------------------------------------------")
                
            print(i+1, img["_id"], img["title"])

            if args.verbose:
                for k in "href", "hrefAnnotated", "hrefOriginal", "hrefThumbnail", "originalFileName", "sanitizedFileName", "title":
                    if k in img:
                        print(k, img[k])
    

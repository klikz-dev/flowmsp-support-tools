#!/usr/bin/env python3

import os
import sys
import pprint
import argparse
import csv
from pymongo import MongoClient
import boto3
import botocore


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="look for duplicate locations by name + address")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("slug", help="customer name, aka slug")

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")

    client = MongoClient(mongo_uri)
    db = client.FlowMSP
    ids = [line.strip() for line in sys.stdin]

    coll = "%s.Location" % args.slug
    rows = deleted = not_found = 0

    for id in ids:
        rows += 1
        row = db[coll].find_one({"_id": id})

        if not row:
            not_found += 1
            print(id, "not found")

        else:
            print(row["_id"],
                  row["address"],
                  row["batchNo"],
                  row["createdOn"],
                  row.get("modifiedOn", ""),
                  row["name"],
                  len(row["images"]))

            r = db[coll].delete_one({"_id": id})
            deleted += 1

    print("rows=%d deleted=%d not_found=%d" % (rows, deleted, not_found))
    

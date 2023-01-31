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
    parser = argparse.ArgumentParser(description="update minimumNewHydrantDistance")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("--slug")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--distance-in-meters", type=int, default=10)

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")
    client = MongoClient(mongo_uri)
    db = client.FlowMSP

    query = {}

    if args.slug:
        query["slug"] = args.slug

    for row in db.Customer.find(query):
        old = row["settings"]["minimumNewHydrantDistance"]
        row["settings"]["minimumNewHydrantDistance"] = args.distance_in_meters

        r = db.Customer.replace_one({"_id": row["_id"]}, row)
        print("updated", row["slug"], old, row["settings"]["minimumNewHydrantDistance"])


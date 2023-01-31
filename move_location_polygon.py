#!/usr/bin/env python3

import os
import sys
import pprint
import argparse
import json
import copy
import csv
from pymongo import MongoClient
import boto3
import botocore


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="move location polygon")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("customer_name", help="customer name, aka slug")
    parser.add_argument("location_id")
    parser.add_argument("latitude_change", type=float)
    parser.add_argument("longitude_change", type=float)
    parser.add_argument("--update", action="store_true")
    parser.add_argument("--revert", action="store_true")

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")

    client = MongoClient(mongo_uri)
    db = client.FlowMSP

    coll = "%s.Location" % args.customer_name

    row = db[coll].find_one({"_id": args.location_id})

    if args.revert:
        row["geoOutline"] = row.pop("geoOutline_orig")
        pprint.pprint(row)

        if args.update:
            r = db[coll].replace_one({"_id": row["_id"]}, row)

        raise SystemExit(0)
    else:
        print("before")
        pprint.pprint(row["geoOutline"])
        print()

        # save a copy so we can revert if needed
        row["geoOutline_orig"] = copy.deepcopy(row["geoOutline"])

        for i, loc in enumerate(row["geoOutline"]["coordinates"][0]):
            print(i, row["geoOutline"]["coordinates"][0][i])
            row["geoOutline"]["coordinates"][0][i][0] -= args.longitude_change
            row["geoOutline"]["coordinates"][0][i][1] -= args.latitude_change
        
        print("after")
        pprint.pprint(row["geoOutline"])
        print()

        pprint.pprint(row)

        if args.update:
            r = db[coll].replace_one({"_id": row["_id"]}, row)
        else:
            print("change not saved, use --update if needed")
        



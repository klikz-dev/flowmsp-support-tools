#!/usr/bin/env python3

import os
import sys
import pprint
import argparse
import csv
import datetime
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
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--multi-line", action="store_true")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--after")
    parser.add_argument("--address")

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")

    client = MongoClient(mongo_uri)
    db = client.FlowMSP

    if args.customer_name == '?':
        for c in db.Customer.find():
            print(c['slug'])

        raise SystemExit(0)

    coll = "DebugInfo"

    if args.after:
        tstamp = datetime.datetime.strptime(args.after, "%Y-%m-%dT%H:%M:%S")

    for rownum, row in enumerate(db[coll].find()):
        if args.after:
            if row["TimeStamp"] >= tstamp:
                pprint.pprint(row)

        else:
            pprint.pprint(row)


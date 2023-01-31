#!/usr/bin/env python3

import os
import sys
import pprint
import argparse
import json
import csv
from pymongo import MongoClient
import boto3
import botocore


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="copy hydrants")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("from_slug")
    parser.add_argument("to_slug")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int)


    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")

    client = MongoClient(mongo_uri)
    db = client.FlowMSP

    from_coll = "%s.Hydrant" % args.from_slug
    to_coll = "%s.Hydrant" % args.to_slug
    rows = 0

    for rownum, row in enumerate(db[from_coll].find()):
        if args.limit and rownum >= args.limit:
            break

        rows += 1

        row["customerSlug"] = args.to_slug

        if args.dry_run is False:
            db[to_coll].insert_one(row)

    print("rows=%d" % rows, file=sys.stderr)

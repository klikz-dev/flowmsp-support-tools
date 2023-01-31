#!/usr/bin/env python3

import os
import sys
import pprint
import csv
import argparse
import re
from pymongo import MongoClient
import boto3
import botocore


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


def process_user(row):
    outrec = {
        "email": row.get("email", ""),
        "firstName": row.get("firstName", ""),
        "lastName": row.get("lastName", ""),
        "role": row.get("role", ""),
        
    }

    if "customerRef" in row:
        outrec["department"] = row["customerRef"].get("customerName", "")
        
    return outrec
    

def get_users(db):
    for row in db.list_collections():
        if row["name"].endswith(".User"):
            for u in db[row["name"]].find():
                yield u

    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="check for broken links")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("--limit", type=int)

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")

    client = MongoClient(mongo_uri)
    db = client.FlowMSP

    hdr = ["email", "firstName", "lastName", "role", "department"]
    w = csv.DictWriter(sys.stdout, hdr, lineterminator="\n")
    w.writeheader()

    for rownum, row in enumerate(get_users(db)):
        if args.limit and rownum >= args.limit:
            break

        w.writerow(process_user(row))

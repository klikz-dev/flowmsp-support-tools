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
import re


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


def get_cuser(db, row):
    if "customerSlug" not in row:
        return

    collection = "%s.User" % row["customerSlug"]

    return db[collection].find_one({"_id": row["_id"]})
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="delete flowmsp admin accounts")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("--slug")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--update", action="store_true")
    parser.add_argument("--debug", action="store_true")

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")
    client = MongoClient(mongo_uri)
    db = client.FlowMSP
    coll = "%s.Location" % args.slug
    rows = updated = 0

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    logging.debug("coll=%s" % coll)

    rows = 0

    hdr = ["_id", "slug", "department", "username", "first_name", "last_name", "role"]
    w = csv.DictWriter(sys.stdout, hdr, lineterminator="\n")
    w.writeheader()

    for rownum, row in enumerate(db.Password.find()):
        if not re.match("admin.*@.*fd\.com", row.get("username", "")):
            continue
        
        crow = get_cuser(db, row)

        if not crow:
            continue

        if crow.get("role", "") != "ADMIN":
            continue

        rows += 1

        if args.limit and rows > args.limit:
            break

        #print("------------------------------------------------------------")
        #pprint.pprint(row)
        #pprint.pprint(crow)

        orec = {
            "_id": row["_id"],
            "slug": row.get("customerSlug", ""),
            "department": crow.get("customerRef", {}).get("customerName", ""),
            "username": row.get("username", ""),
            "first_name": crow.get("firstName", ""),
            "last_name": crow.get("lastName", ""),
            "role": crow.get("role", "")
        }
        
        w.writerow(orec)

    logging.info("rows=%d updated=%d" % (rows, updated))



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
from utils import get_db


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


def get_date(row, key):
    if key in row:
        return row[key].strftime("%Y-%m-%d %H:%M:%S")
    else:
        return


def get_user(db, userid):
    if not userid:
        return ""
    else:
        row = db.Password.find_one({"_id": userid})

        if row:
            return row.get("username")
        else:
            return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="check for broken links")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("slug", help="customer name, aka slug")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--limit", type=int)

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")

    client = MongoClient(mongo_uri)
    db = client.FlowMSP

    coll = "%s.Hydrant" % args.slug

    hdr = ["_id",
           "batchNo",
           "createdBy",
           "createdOn",
           "flow",
           "color",
           "lat",
           "lon",
           "modifiedBy",
           "modifiedOn",
           "size",
           "streetAddress"]

    w = csv.DictWriter(sys.stdout, hdr, lineterminator="\n")
    w.writeheader()

    for rownum, row in enumerate(db[coll].find()):
        if args.limit and rownum >= args.limit:
            break

        orec = {
            "_id": row["_id"],
            "batchNo": row.get("batchNo", ""),
            "createdBy": get_user(db, row.get("createdBy", "")),
            "createdOn": get_date(row, "createdOn"),
            "flow": row.get("flow", ""),
            "color": row["flowRange"]["pinColor"],
            "lat": row["lonLat"]["coordinates"][1],
            "lon": row["lonLat"]["coordinates"][0],
            "modifiedBy": get_user(db, row.get("modifiedBy", "")),
            "modifiedOn": get_date(row, "modifiedOn"),
            "size": row.get("size", ""),
            "streetAddress": row.get("streetAddress", "")
        }

        w.writerow(orec)


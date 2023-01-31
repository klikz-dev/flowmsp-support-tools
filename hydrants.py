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
    parser.add_argument("customer_name", help="customer name, aka slug")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--multi-line", action="store_true")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--truncate", action="store_true")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--batch-no", type=int)

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")

    client = MongoClient(mongo_uri)
    db = client.FlowMSP

    coll = "%s.Hydrant" % args.customer_name

    if args.truncate:
        q = {}

        if args.batch_no:
            q["batchNo"] = args.batch_no

        result = db[coll].delete_many(q)
        print("deleted %d rows" % result.deleted_count)
        raise SystemExit(0)

    if args.multi_line is False:
        w = csv.writer(sys.stdout, lineterminator="\n")

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
               "streetAddress",
               "inService",
               "outServiceDate"]

        w.writerow(hdr)

    output = []

    for rownum, row in enumerate(db[coll].find()):
        if args.limit and rownum >= args.limit:
            break

        if args.multi_line:
            for k in "createdOn", "modifiedOn":
                if k in row:
                    row[k] = str(row[k])
            output.append(row)
        else:
            orec = [row["_id"],
                    row.get("batchNo", ""),
                    get_user(db, row.get("createdBy", "")),
                    get_date(row, "createdOn"),
                    row.get("flow", ""),
                    row["flowRange"]["pinColor"],
                    row["lonLat"]["coordinates"][1],
                    row["lonLat"]["coordinates"][0],
                    get_user(db, row.get("modifiedBy", "")),
                    get_date(row, "modifiedOn"),
                    row.get("size", ""),
                    row.get("streetAddress", ""),
                    row.get("inService", ""),
                    row.get("outServiceDate", "")]

            w.writerow(orec)

    if args.multi_line:
        json.dump(output, sys.stdout, indent=4)

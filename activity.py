#!/usr/bin/env python3

import os
import sys
import pprint
import csv
import argparse
import datetime
import pymongo
from pymongo import MongoClient
import boto3
import botocore
import logging

logging.basicConfig()
logger = logging.getLogger("activity")


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


def dispatch_report(w, rownum, row, args):
    if rownum == 0 and not args.multi_line:
        w.writerow(["Source", "TimeStamp", "ErrorFlag", "ErrorDescription"])

    if args.multi_line:
        pprint.pprint(row)
    else:
        w.writerow([row["Source"],
                    row["TimeStamp"],
                    row["ErrorFlag"],
                    row["ErrorDescription"]])

    if args.debug:
        pprint.pprint(row)

    return


def activity_report(w, rownum, row, args):
    if rownum == 0 and not args.multi_line:
        w.writerow(["_id", "customer", "user_name", "subject", "source", "tstamp", "version"])

    if args.multi_line:
        pprint.pprint(row)
        return

    if args.debug:
        pprint.pprint(row)

    source = row["Source"]

    # avoid printing so many values of mobile
    if args.clean_source:
        source = source.split("-")[0]

    w.writerow([row["_id"],
                row.get("Details", {}).get("customerSlug", ""),
                row.get("Details", {}).get("userName", ""),
                row.get("Details", {}).get("subject", ""),
                source,
                row["TimeStamp"],
                row.get("Details", {}).get("version", "")])

    if args.debug:
        pprint.pprint(row)

    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dump info from DebugInfo collection")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("--slug")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--multi-line", action="store_true")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--dispatch", action="store_true")
    parser.add_argument("--source")
    parser.add_argument("--subject")
    parser.add_argument("--start")
    parser.add_argument("--type", choices=["EMAIL", "SMS"])
    parser.add_argument("--end")
    parser.add_argument("--clean-source", action="store_true")
    parser.add_argument("--delete", action="store_true")
    parser.add_argument("--sort-order", type=int,
                        choices=[pymongo.ASCENDING, pymongo.DESCENDING],
                        default=pymongo.ASCENDING)

    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    logger.info(args)

    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")
    client = MongoClient(mongo_uri)
    db = client.FlowMSP

    coll = "DebugInfo"

    query = {}

    if args.start:
        query["TimeStamp"] = {"$gte": datetime.datetime.strptime(args.start, "%Y-%m-%d")}

    if args.end:
        if "TimeStamp" not in query:
            query["TimeStamp"] = {}

        query["TimeStamp"]["$lt"] = datetime.datetime.strptime(args.end, "%Y-%m-%d")

    if args.dispatch:
        query["Source"] = "DISPATCH"
        f = dispatch_report
    else:
        query["Source"] = {"$ne": "DISPATCH"}
        f = activity_report

    if args.type:
        query["Details.Type"] = args.type

    if args.source:
        query["Source"] = args.source

    if args.subject:
        query["Details.subject"] = args.subject

    if args.slug is not None:
        if args.slug == "":
            query["Details.customerSlug"] = None
        else:
            query["Details.customerSlug"] = args.slug

    if args.debug:
        logger.debug(debug)

    w = csv.writer(sys.stdout)

    logger.info(query)

    orecs = deleted = 0

    for rownum, row in enumerate(db.DebugInfo.find(query)):
        if args.limit and rownum > args.limit:
            break

        f(w, rownum, row, args)
        orecs += 1

        if args.delete:
            r = db.DebugInfo.delete_one({"_id": row["_id"]})
            deleted += r.deleted_count

    logger.info("wrote=%d deleted=%d" % (orecs, deleted))

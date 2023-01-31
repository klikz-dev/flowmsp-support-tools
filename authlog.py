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
        w.writerow(["_id", "customer", "subject", "source", "tstamp", "version"])

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
                row["Details"]["customerSlug"],
                row["Details"]["subject"],
                source,
                row["TimeStamp"],
                row["Details"]["version"]])

    if args.debug:
        pprint.pprint(row)

    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dump info from AuthLog collection")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("--slug")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--multi-line", action="store_true")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--dispatch", action="store_true")
    parser.add_argument("--start")
    parser.add_argument("--end")
    parser.add_argument("--sort-order", type=int,
                        choices=[pymongo.ASCENDING, pymongo.DESCENDING],
                        default=pymongo.ASCENDING)

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")
    client = MongoClient(mongo_uri)
    db = client.FlowMSP

    hdr = ["ymd", "username", "tstamp", "resultCode", "remoteAddr", "companySlug"]
    w = csv.DictWriter(sys.stdout, hdr, lineterminator="\n")
    w.writeheader()

    for rownum, row in enumerate(db.AuthLog.find()):
        ymd = row["_id"].strftime("%Y-%m-%d")

        if args.start and ymd < args.start:
            continue

        if args.end and ymd > args.end:
            continue

        for att in row["authAttempts"]:
            orec = {
                "ymd": ymd,
                "username": att["username"],
                "tstamp": att["timestamp"].strftime("%Y-%m-%d %H:%M:%S"),
                "resultCode": att["resultCode"],
                "remoteAddr": att["remoteAddr"],
                "companySlug": att.get("companySlug", "")
            }

            w.writerow(orec)

    raise SystemExit(0)


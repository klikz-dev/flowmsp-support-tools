#!/usr/bin/env python3

import os
import sys
import pprint
import argparse
import csv
from pymongo import MongoClient
import boto3
import botocore
import tabulate


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="check for broken links")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--multi-line", action="store_true")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--summary", action="store_true")
    parser.add_argument("--tabulate", action="store_true")
    parser.add_argument("--csv", action="store_true")

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")
    client = MongoClient(mongo_uri)
    db = client.FlowMSP

    hdr = ["Customer",
           "msgs",
           "emailFormat",
           "emailGateway",
           "emailSignature",
           "emailSignatureLocation",
           "fromContains",
           "fromNotContains",
           "toContains",
           "bodyContains",
           "bodyNotContains"]

    summary = {}
    results = []

    for rownum, row in enumerate(db["Customer"].find()):
        if row.get("emailFormat", "") != "":
            slug = row.get("slug", "")
            msgs = db["%s.MsgReceiver" % slug].count_documents({})

            results.append([slug,
                            msgs,
                            row.get("emailFormat",            ""),
                            row.get("emailGateway",           ""),
                            row.get("emailSignature",         ""),
                            row.get("emailSignatureLocation", ""),
                            row.get("fromContains", ""),
                            row.get("fromNotContains", ""),
                            row.get("toContains", ""),
                            row.get("bodyContains", ""),
                            row.get("bodyNotContains", "")])

            k = row.get("emailFormat", "")
            if k not in summary:
                summary[k] = []

            summary[k].append(row.get("slug", ""))

    if args.tabulate:
        print(tabulate.tabulate(results, headers=hdr, tablefmt="psql"))
    elif args.csv:
        w = csv.writer(sys.stdout, lineterminator="\n")

        w.writerow(hdr)

        for r in results:
            w.writerow(r)

    # m = max([len(f) for f in summary])

    #for fmt in sorted(summary):
    #    print("%-*.*s" % (m, m, fmt), "%3d" % len(summary[fmt]), summary[fmt])

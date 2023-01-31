#!/usr/bin/env python3

import os
import sys
import uuid
import pprint
import time
import argparse
import csv
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
    parser.add_argument("infile")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--multi-line", action="store_true")
    parser.add_argument("--debug", action="store_true")

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")
    PGM = os.path.basename(sys.argv[0])

    client = MongoClient(mongo_uri)
    db = client.FlowMSP

    fp = open(args.infile, errors="ignore")
    r = csv.DictReader(fp)

    batchNo = time.time_ns()
    collection = "%s.Location" % args.customer_name
    rows = 0

    for rownum, rec in enumerate(r):
        rows += 1
        # print(rownum, rec)

        row = {
            "_id": str(uuid.uuid4()),
            "address": {
                "address1": rec["Street Address"],
                "city": rec["City"],
                "state": rec["State"],
                "zip": rec["Zip code"]
            },
            "batchNo": batchNo,
            "createdOn": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "createdBy": PGM,
            "customerSlug": args.customer_name,
            "name": rec["Name"],
            "images": [],
            "hydrants": [],
        }

        db[collection].insert_one(row)
        # pprint.pprint(row)

    print("rows=%d" % rows, file=sys.stderr)

    raise SystemExit(0)


    for rownum, row in enumerate(db[coll].find()):
        if args.address and not addr1.startswith(args.address):
            continue

        if args.multi_line:
            pprint.pprint(row)
        else:
            w.writerow([rownum,
                        row["_id"],
                        row["batchNo"],
                        row["createdOn"],
                        row["modifiedOn"],
                        row.get("name",""),
                        row["address"]["address1"],
                        row["address"]["city"],
                        row["address"]["state"],
                        "%d images" % len(row["images"])])
            polygon = row["geoOutline"]["coordinates"][0]

            """
            print(polygon)

            print("lng max - min",
                  max([lng for lng, lat in polygon]) - min([lng for lng, lat in polygon]))

            print("lat max - min",
                  max([lat for lng, lat in polygon]) - min([lat for lng, lat in polygon]))
            """

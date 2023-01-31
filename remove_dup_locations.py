#!/usr/bin/env python3

import os
import sys
import pprint
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
    parser = argparse.ArgumentParser(description="look for duplicate locations by name + address")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("customer_name", help="customer name, aka slug")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--multi-line", action="store_true")
    parser.add_argument("--remove", action="store_true")

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")

    client = MongoClient(mongo_uri)
    db = client.FlowMSP

    coll = "%s.Location" % args.customer_name

    locations = {}
    inrecs = dups = 0

    # look for duplicates by address
    for row in db[coll].find():
        inrecs += 1

        key = (row.get("name",""),
               row["address"]["address1"],
               row["address"]["city"],
               row["address"]["state"])

        key = row["address"]["address1"]

        if key not in locations:
            locations[key] = []

        locations[key].append(row)
    # end for
    
    """
    we only care about dups that have some auto-generated locations,
    identified by batchNo != 0

    - if a set of dups are all batchNo != 0 we leave only one

    """
    deleted = 0

    for key in locations:
        if len(locations[key]) == 1:
            continue

        batch_numbers = {r["batchNo"] for r in locations[key]}

        # if all rows are batchNo=0, leave alone
        if batch_numbers == {0}:
            print(len(locations[key]), key, batch_numbers)
            continue

        print("------------------------------------------------------------")
        print(len(locations[key]), key, batch_numbers)
        dups += 1

        # if set of dups are mixed, delete all batchNo != 0
        if 0 in batch_numbers:
            for i, r in enumerate(locations[key]):
                if r["batchNo"] == 0:
                    disp = "keep"
                else:
                    disp = "delete"

                print(i, disp, r["_id"], r["batchNo"], r.get("name", ""), r["address"])

                if disp == "delete":
                    deleted += 1

                    if args.remove:
                        db[coll].delete_one({"_id": r["_id"]})

            continue

        # if all dups were auto-generated, keep all non-blank addresses
        for i, r in enumerate(locations[key]):
            if r["address"]["address1"].strip() == "":
                disp = "delete"
            else:
                disp = "keep"

            print(i, disp, r["_id"], r["batchNo"], r.get("name" ""), r["address"])
            
            if disp == "delete":
                deleted += 1

                if args.remove:
                    db[coll].delete_one({"_id": r["_id"]})
        print()

    print("inrecs=%d dups=%d deleted=%d" % (inrecs, dups, deleted))
    

#!/usr/bin/env python3

import os
import sys
import pprint
import argparse
from pymongo import MongoClient
import boto3
import botocore


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


def update_flowrate(db, c, newvalue):
    if ("settings" in c and
        "preplanningMaxAreaForFlowComputation" in c["settings"]):
        oldvalue = c["settings"]["preplanningMaxAreaForFlowComputation"]

        print(c["slug"], "old", oldvalue)

        if oldvalue != newvalue:
            c["settings"]["preplanningMaxAreaForFlowComputation"] = newvalue
            db.Customer.replace_one({"_id": c["_id"]}, c)
            print(c["slug"], "updated")
            

    return


def update_locations(db, c):
    for row in db["%s.Location" % c["slug"]].find():
        roo
        if needs_update:
            id = row["_id"]
            db["%s.Location" % args.slug].replace_one({"_id": row["_id"]}, row)
            corrected += 1
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="check for broken links")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("value", type=int)
    parser.add_argument("customer_name", help="customer name, aka slug",
                        nargs='*')

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")
    client = MongoClient(mongo_uri)
    db = client.FlowMSP

    for c in db.Customer.find():
        if args.customer_name and c["slug"] not in args.customer_name:
            continue

        update_flowrate(db, c, args.value)
        update_locations(db, c)

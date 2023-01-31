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


def_colors = {'rangePinColors': [{'high': 500,
                                  'label': '0 to less than 500 GPM',
                                  'low': 0,
                                  'pinColor': 'RED'},
                                 {'high': 1000,
                                  'label': '500 to less than 1000 GPM',
                                  'low': 500,
                                  'pinColor': 'ORANGE'},
                                 {'high': 1500,
                                  'label': '1000 to less than 1500 GPM',
                                  'low': 1000,
                                  'pinColor': 'GREEN'},
                                 {'high': 100000,
                                  'label': '1500+ GPM',
                                  'low': 1500,
                                  'pinColor': 'BLUE'}],
              'unknownPinColor': {'high': 0,
                                  'label': 'Unknown',
                                  'low': 0,
                                  'pinColor': 'YELLOW'}
}

def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


def get_customer(db, slug):
    return db.Customer.find_one({"slug": slug})


def set_flow_range(flow, pin_legend):
    for p in pin_legend["rangePinColors"]:
        if flow < p["high"]:
            return {
                "label": p["label"],
                "pinColor": p["pinColor"]
            }

    return {
        "label": pin_legend["unknownPinColor"]["label"],
        "pinColor": pin_legend["unknownPinColor"]["pinColor"]
    }


def get_db(profile):
    session = boto3.session.Session(profile_name=profile)
    mongo_uri = get_parm(session, "mongo_uri")
    client = MongoClient(mongo_uri)

    return client.FlowMSP


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="set hydrant flow range")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("slug")
    parser.add_argument("flow", type=int)
    parser.add_argument("--limit", type=int)

    args = parser.parse_args()
    db = get_db(args.profile)

    c = get_customer(db, args.slug)

    flow_range = set_flow_range(args.flow, c.get("pinLegend", def_colors))

    coll = "%s.Hydrant" % args.slug

    for rownum, row in enumerate(db[coll].find()):
        if args.limit and rownum >= args.limit:
            break

        print("------------------------------------------------------------")
        pprint.pprint(row)
        print("------------------------------------------------------------")

        row["flow"] = args.flow
        row["flowRange"] = flow_range
        pprint.pprint(row)
        print("------------------------------------------------------------")

        r = db[coll].replace_one({"_id": row["_id"]}, row)

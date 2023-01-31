#!/usr/bin/env python3

import os
import sys
import pprint
import argparse
from pymongo import MongoClient
import boto3
import botocore
import geojson


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="set dispatcher fields")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("geojson_file")
    parser.add_argument("--list", action="store_true")
    parser.add_argument("--collection", default="FireDistricts")

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")
    client = MongoClient(mongo_uri)
    db = client.FlowMSP

    data = geojson.load(open(args.geojson_file))

    if args.list:
        for i, f in enumerate(data["features"]):
            print(i, f["properties"], f["coordinates"]["type"])

        raise SystemExit(0)

    print(args.index, data["features"][args.index]["properties"])
    row["districtOutline"] = data["features"][args.index]["geometry"]

    pprint.pprint(row)

    if args.update:
        r = db.Customer.replace_one({"_id": row["_id"]}, row)
        print("updated", r)
    else:
        print("nothing updated")



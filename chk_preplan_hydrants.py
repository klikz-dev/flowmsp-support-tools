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
from geopy.distance import geodesic


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


def load_hydrants(db, slug):
    coll = "%s.Hydrant" % slug
    tbl = {}

    for row in db[coll].find():
        id = row["_id"]
        tbl[id] = row

    return tbl


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="check for broken links")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("customer_name", help="customer name, aka slug")

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")
    client = MongoClient(mongo_uri)
    db = client.FlowMSP

    hydrants = load_hydrants(db, args.customer_name)

    coll = "%s.Location" % args.customer_name

    for rownum, row in enumerate(db[coll].find()):
        if "hydrants" not in row:
            continue

        lat = row["geoOutline"]["coordinates"][0][0][1]
        lon = row["geoOutline"]["coordinates"][0][0][0]
        loc = (lat, lon)

        results = []

        for i, h in enumerate(row["hydrants"]):
            hydrant = hydrants[h]
            lat = hydrant["lonLat"]["coordinates"][1]
            lon = hydrant["lonLat"]["coordinates"][0]
            hyd = (lat, lon)

            r = [i, hydrant.get("streetAddress", ""), hyd, geodesic(loc, hyd).feet]

            if r[-1] > 1000:
                results.append(r)

        if results:
            print("------------------------------------------------------------")
            print("id", row["_id"])
            print(row.get("name", ""), row.get("address", {}), loc)

            for r in results:
                print(r)

            print()

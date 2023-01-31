#!/usr/bin/env python3

import os
import sys
import pprint
import argparse
import pprint
import csv
import json
from pymongo import MongoClient
import boto3
import botocore


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="dump customer info to display on map")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")

    client = MongoClient(mongo_uri)
    db = client.FlowMSP

    w = csv.writer(sys.stdout, lineterminator="\n")
    hdr = ["slug", "name", "city", "state", "lat", "lon"]
    w.writerow(hdr)

    for c in db.Customer.find():
        try:
            lat = c["address"]["latLon"]["coordinates"][1]
            lon = c["address"]["latLon"]["coordinates"][0]
        except:
            print(c["name"], "no lat/lon", file=sys.stderr)
            continue
            
        orec = [c.get("slug", ""),
                c.get("name", ""),
                c.get("city", ""),
                c.get("state", ""),
                lat,
                lon]

        w.writerow(orec)


        

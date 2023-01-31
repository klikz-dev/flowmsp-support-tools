#!/usr/bin/env python3

import os
import sys
import csv
import re
import time
import argparse
import requests
import pprint
import boto3
import xml.etree.ElementTree as ET
from collections import namedtuple
from pymongo import MongoClient
import shapely
import shapely.geometry


def get_outline(db, lat, lon):
    query = {
        "geometry": {
            "$geoIntersects": {
                "$geometry": {
                    "type": "Point",
                    "coordinates": [ lon, lat ]
                }
            }
        }
    }

    return db.ms_geodata.find_one(query)


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="update polygons with pre-traced ones where available")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("slug")
    parser.add_argument("--limit", type=int)

    args = parser.parse_args()
    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")
    client = MongoClient(mongo_uri)
    db = client.FlowMSP
    collection = "%s.Location" % args.slug
    inrecs = outrecs = 0

    for rownum, row in enumerate(db[collection].find()):
        if args.limit and rownum >= args.limit:
            break

        inrecs += 1
        s = shapely.geometry.asShape(row["geoOutline"])

        # lon, lat = row["geoOutline"]["coordinates"][0][0]
        lat = s.centroid.y
        lon = s.centroid.x
        
        outline = get_outline(db, lat, lon)

        print(rownum, lat, lon, row["address"], outline is not None)

        if outline:
            outrecs += 1
            pprint.pprint(row["geoOutline"])
            print()
            pprint.pprint(outline)

            row["orig_roofArea"] = row.pop("roofArea")
            row["orig_requiredFlow"] = row.pop("requiredFlow")
            row["orig_geoOutline"] = row["geoOutline"]
            row["geoOutline"] = outline["geometry"]
            db[collection].replace_one({"_id": row["_id"]}, row)

    print("in=%d out=%d" % (inrecs, outrecs), file=sys.stderr)

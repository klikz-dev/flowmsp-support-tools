#!/usr/bin/env python3

import os
import sys
import uuid
import pprint
import time
import json
import argparse
import csv
import datetime
from pymongo import MongoClient
import boto3
import botocore
import shapely
import shapely.geometry
import requests
from geopy import Here
from geopy import GoogleV3


def geocode(row, mapkey):
    # g = Here("WaGiLAKzJQzySaXFJm9o", "bO4lKri2pJuiCxnRuD0-KQ")
    g = GoogleV3(mapkey)

    # pprint.pprint(row)

    address = ", ".join([
        row["Street Address"],
        row["City"],
        row["State"],
        row["Zip code"]])

    r = g.geocode(address, timeout=15)

    return r.latitude, r.longitude


def get_polygon_string(outline):
    points = []

    for lon, lat in outline["coordinates"][0]:
        p = ":".join([str(lat), str(lon)])
        points.append(p)

    return "|".join(points)


def get_outline(collection, lat, lon):
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

    return collection.find_one(query)


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="load RMS data")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--footprints-collection", default="ms_geodata")
    parser.add_argument("--limit", type=int)

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")
    map_key = get_parm(session, "GOOGLE_MAP_API_KEY")

    client = MongoClient(mongo_uri)
    db = client.FlowMSP

    footprints = db[args.footprints_collection]

    inrecs = updated = 0

    r = csv.DictReader(sys.stdin)
    w = csv.DictWriter(sys.stdout, r.fieldnames, lineterminator="\n")
    w.writeheader()

    for rownum, rec in enumerate(r):
        if args.limit and rownum >= args.limit:
            break

        inrecs += 1

        lat, lon = geocode(rec, map_key)

        print("record", inrecs, "lat", lat, "lon", lon, file=sys.stderr)

        outline = get_outline(footprints, lat, lon)

        if outline and "geometry" in outline:
            updated += 1
            rec["Polygon"] = get_polygon_string(outline["geometry"])

        w.writerow(rec)

    print("inrecs=%d updated=%d" % (inrecs, updated), file=sys.stderr)

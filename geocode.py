#!/usr/bin/env python3

import os
import sys
import uuid
import pprint
import time
import json
import argparse
import csv
import urllib.parse
from pymongo import MongoClient
import boto3
import botocore
import requests


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


base_url = "https://maps.googleapis.com/maps/api/geocode/json"


def geocode(address, map_key):
    a = address.replace(" ", "+")
    a2 = urllib.parse.quote(a, safe="+")
    url = "{u}?address={a}&key={k}".format(u=base_url, a=a2, k=map_key)
    r = requests.get(url)

    if r.ok:
        return r


def reverse_geocode(polygon, map_key):
    polygon = shapely.geometry.asShape(polygon)
    p = polygon.centroid
    url = "{url}?latlng={lat},{lon}&key={key}".format(
        url=base_url,
        lat=p.y,
        lon=p.x,
        key=map_key)

    r = requests.get(url)

    if not r.ok:
        return

    jdata = r.json()

    # this will need some work
    # for now, return the first street address
    for r in jdata["results"]:
        #pprint.pprint(r)

        if "street_address" in r["types"]:
            return get_address(r)

    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="check for broken links")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("address_fields", nargs="+")
    parser.add_argument("--delimiter", default=",")
    parser.add_argument("--limit", type=int)
    
    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    map_key = get_parm(session, "GOOGLE_MAP_API_KEY")

    sys.stdin = open(sys.stdin.fileno(), errors="replace", encoding="utf8", buffering=1)

    r = csv.DictReader(sys.stdin)
    hdr = r.fieldnames + ["addr", "lat", "lon"]

    w = csv.DictWriter(sys.stdout, hdr, delimiter=args.delimiter, lineterminator="\n")
    w.writeheader()

    for recnum, rec in enumerate(r):
        if args.limit and recnum >= args.limit:
            break

        rec["addr"] = " ".join([rec[a].strip() for a in args.address_fields])
        rec["lat"] = ""
        rec["lon"] = ""

        r = geocode(rec["addr"], map_key)

        if r.ok:
            results = r.json()

            try:
                latlon = results.get("results", [{}])[0].get("geometry", {}).get("location", {})
                rec["lat"] = latlon.get("lat", "")
                rec["lon"] = latlon.get("lng", "")
            except:
                print(recnum, rec, file=sys.stderr)
                print(results, file=sys.stderr)
                pass

        w.writerow(rec)

    

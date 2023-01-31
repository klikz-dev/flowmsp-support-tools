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


google_base_url = "https://maps.googleapis.com/maps/api/geocode/json"

def reverse_geocode(lat, lon, map_key):
    url = "{url}?latlng={lat},{lon}&key={key}".format(
        url=google_base_url, lat=lat, lon=lon, key=map_key)

    r = requests.get(url)

    return r


def get_info(r, key):
    for x in r:
        if key in x["types"]:
            return x["short_name"]

    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="check for broken links")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("--latitude", default="Lat")
    parser.add_argument("--longitude", default="Lon")
    parser.add_argument("--delimiter", default=",")
    parser.add_argument("--limit", type=int)

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    map_key = get_parm(session, "GOOGLE_MAP_API_KEY")

    sys.stdin = open(sys.stdin.fileno(), errors="replace", encoding="utf8", buffering=1)

    r = csv.DictReader(sys.stdin)
    hdr = r.fieldnames + ["formatted_address", "city", "state", "zip"]

    w = csv.DictWriter(sys.stdout, hdr, delimiter=args.delimiter, lineterminator="\n")
    w.writeheader()

    for recnum, rec in enumerate(r):
        if args.limit and recnum >= args.limit:
            break

        r = reverse_geocode(rec[args.latitude], rec[args.longitude], map_key)

        if r.ok:
            results = r.json()

            rec["formatted_address"] = results["results"][0]["formatted_address"]
            rec["city"] = get_info(results["results"][0]["address_components"], "locality")
            rec["state"] = get_info(results["results"][0]["address_components"], "administrative_area_level_1")
            rec["zip"] = get_info(results["results"][0]["address_components"], "postal_code")
            
        w.writerow(rec)

    

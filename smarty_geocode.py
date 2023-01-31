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
import logging


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


def smartystreets_geocode(address1, city, state):
    addr = urllib.parse.quote(address1.replace(" ", "+"), safe="+")
    city = urllib.parse.quote(city.replace(" ", "+"), safe="+")
    auth_id = "73eed5a6-a281-351f-38c8-330d8295173b"
    auth_token = "bzhlCsgaYFVMYDDKf2gB"
    url = "https://us-street.api.smartystreets.com/street-address?auth-id={auth_id}&auth-token={token}".format(
        auth_id=auth_id, token=auth_token)

    url += "&street={addr}&city={city}&state={st}&candidates=10".format(
        addr=addr, city=city, st=state)

    url += "&license=us-rooftop-geo-cloud"

    r = requests.get(url)

    return r


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="check for broken links")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("--address_fields", default="Street Address")
    parser.add_argument("--city_field", default="City")
    parser.add_argument("--state_field", default="State")
    parser.add_argument("--delimiter", default=",")
    parser.add_argument("--limit", type=int)

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)

    logging.basicConfig(level=logging.INFO)

    sys.stdin = open(sys.stdin.fileno(), errors="replace", encoding="utf8", buffering=1)

    r = csv.DictReader(sys.stdin)
    hdr = r.fieldnames + ["addr", "lat", "lon", "precision"]

    w = csv.DictWriter(sys.stdout, hdr, delimiter=args.delimiter, lineterminator="\n")
    w.writeheader()

    for recnum, rec in enumerate(r):
        if args.limit and recnum >= args.limit:
            break

        rec["addr"] = " ".join([rec[a].strip() for a in args.address_fields.split(",")])
        rec["lat"] = ""
        rec["lon"] = ""
        rec["precision"] = ""

        r = smartystreets_geocode(rec["addr"],
                                  rec[args.city_field],
                                  rec[args.state_field])

        if r.ok:
            results = r.json()

            try:
                rec["lat"] = results[0].get("metadata", {}).get("latitude", "")
                rec["lon"] = results[0].get("metadata", {}).get("longitude", "")
                rec["precision"] = results[0].get("metadata", {}).get("precision", "")

            except:
                logging.info("%d: failure" % recnum)
                logging.info(results)
                pass

        w.writerow(rec)

    

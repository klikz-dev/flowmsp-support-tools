#!/usr/bin/env python3

import os
import sys
import pprint
import argparse
import pprint
import csv
import datetime
import json
from pymongo import MongoClient
import boto3
import botocore


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


def get_s3_date(url):
    _, _, _, bucket, key = url.split("/", 4)

    s3 = boto3.client("s3")

    try:
        o = s3.get_object(Bucket=bucket, Key=key)
    except:
        print("error", url, file=sys.stderr)
        return

    if "LastModified" in o:
        dt = o["LastModified"].strftime("%Y-%m-%d")

        return dt

    return


def create_report(db, slug, counts):
    coll = "%s.Location" % slug
    skipped = 0

    print("slug", s, file=sys.stderr)

    for rownum, row in enumerate(db[coll].find()):
        print(rownum, file=sys.stderr)

        if "createdOn" not in row:
            skipped += 1
            continue

        dt = row["createdOn"].strftime("%Y-%m-%d")

        if dt not in counts:
            counts[dt] = {"locations": 0, "images": 0, "annotations": 0}

        counts[dt]["locations"] += 1

        for img in row.get("images", []):
            if "hrefOriginal" in img:
                dt = get_s3_date(img["hrefOriginal"])
                
                if dt not in counts:
                    counts[dt] = {"locations": 0, "images": 0, "annotations": 0}

                counts[dt]["images"] += 1

            if "hrefAnnotated" in img:
                dt = get_s3_date(img["hrefAnnotated"])

                if dt not in counts:
                    counts[dt] = {"locations": 0, "images": 0, "annotations": 0}

                counts[dt]["annotations"] += 1

    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="activity report")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("slug", nargs='+', help="pekinfiredep, lockporttown")

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")
    image_uri = get_parm(session, "image_uri")

    client = MongoClient(mongo_uri)

    db = client.FlowMSP
    counts = {}

    for s in args.slug:
        counts[s] = {}

        create_report(db, s, counts[s])

    hdr = ["dt", "slug", "locations", "images", "annotations"]
    w = csv.DictWriter(sys.stdout, hdr, lineterminator="\n")
    w.writeheader()

    for s in counts:
        for dt in counts[s]:
            orec = counts[s][dt]
            orec["dt"] = dt
            orec["slug"] = s

            w.writerow(orec)

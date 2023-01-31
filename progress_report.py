#!/usr/bin/env python3

import os
import sys
import pprint
import argparse
import csv
import random
from pymongo import MongoClient
import boto3
import botocore


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


def traced(loc):
    if "geoOutline" in loc:
        return "Y"
    else:
        return "N"
    

def get_num_photos(loc):
    return len(loc.get("images", []))


def get_num_annotations(loc):
    images = loc.get("images", [])

    annotated = list(filter(lambda i: "annotationMetadata" in i, images))

    if len(annotated) >= 4:
        return "Y"
    else:
        return "N"
 

def has_building_data(loc):
    if "building" in loc:
        return "Y"
    else:
        return "N"
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="set dispatcher fields")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("slug")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--num-teams", type=int, default=5)

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")
    client = MongoClient(mongo_uri)
    db = client.FlowMSP
    counts = {}

    w = csv.writer(sys.stdout, lineterminator="\n")
    w.writerow([
        "Name",
        "Address1",
        "Team",
        "Traced",
        "4+ Photos",
        "4+ Annotations",
        "Building Data",
        "% Complete"
    ])
            
    g = random.Random()

    for rownum, row in enumerate(db["%s.Location" % args.slug].find()):
        if args.limit and rownum >= args.limit:
            break

        w.writerow([row["name"],
                    row["address"]["address1"],
                    g.randint(1,args.num_teams),
                    traced(row),
                    get_num_photos(row),
                    get_num_annotations(row),
                    has_building_data(row)])

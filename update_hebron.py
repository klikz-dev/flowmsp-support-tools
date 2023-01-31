#!/usr/bin/env python3

import os
import sys
import pprint
import argparse
import csv
from pymongo import MongoClient
import boto3
import botocore

class Rec:
    def __init__(self, flds):
        self._id = flds[0]
        self.flow = flds[1]
        self.color = flds[2]
        self.lon = flds[3]
        self.lat = flds[4]


data = [Rec(["4143cdb3-e921-41cf-b994-d8b49fbddd7e", 1062, "green", -87.2031071, 41.33388826]),
        Rec(["7faf4cf4-b37d-43a9-841e-0e321a5552b0", 1188, "green", -87.20118027, 41.33393761]),
        Rec(["0606cb7b-0aba-4eef-b450-011d243f1c2f",  920, "orange", -87.20110483, 41.3198736]),
        Rec(["c114ce37-f621-4f27-ba48-ad2830a0c64b", 1188, "green", -87.20042825, 41.32822138])]

def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


def record_we_want(row):
    for d in data:
        if row["_id"] == d._id:
            return d

    return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="check for broken links")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")

    client = MongoClient(mongo_uri)
    db = client.FlowMSP

    slug = "hebronfirede"

    coll = "%s.Hydrant" % slug

    for rownum, row in enumerate(db[coll].find()):
        d = record_we_want(row)

        if d:
            print(row["_id"],
                  row["lonLat"],
                  row["notes"],
                  row["streetAddress"])

            

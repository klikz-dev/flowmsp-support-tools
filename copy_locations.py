#!/usr/bin/env python3

import os
import sys
import pprint
import argparse
from pymongo import MongoClient
import boto3
import botocore

def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


class EnvInfo:
    def __init__(self, profile_name):
        self.profile_name = profile_name
        self.session = boto3.session.Session(profile_name=profile_name)
        self.s3url = get_parm(self.session, "image_uri")
        self.mongo_url = get_parm(self.session, "mongo_uri")
        self.client = MongoClient(self.mongo_url)
        self.db = self.client.FlowMSP


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="copy data from prod to test")
    parser.add_argument("src_slug")
    parser.add_argument("dst_slug")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--limit", action="store_true")
    parser.add_argument("--src", default="flowmsp-prod")
    parser.add_argument("--dst", default="flowmsp-dev")

    parser.add_argument("--skip-images", action="store_true")

    args = parser.parse_args()

    src = EnvInfo(args.src)
    dst = EnvInfo(args.dst)
    rows = 0

    for loc in src.db["%s.Location" % args.src_slug].find():
        loc.pop("requiredFlow")
        loc.pop("roofArea")

        rows += 1
        dst.db["%s.Location" % args.dst_slug].insert_one(loc)

    print("copied %d locations" % rows)


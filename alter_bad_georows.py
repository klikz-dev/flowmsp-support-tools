#!/usr/bin/env python3

import os
import sys
import json
import time
import argparse
import pprint
from pymongo import MongoClient
from bson import ObjectId
import boto3
import botocore

def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


def get_db(profile):
    session = boto3.session.Session(profile_name=profile)
    mongo_uri = get_parm(session, "mongo_uri")

    client = MongoClient(mongo_uri)
    db = client.FlowMSP
    return db


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="rename geometry field for bad row")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("object_ids", nargs="+")

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")

    client = MongoClient(mongo_uri)
    db = client.FlowMSP

    for object_id in args.object_ids:
        print(object_id)
        row = db.ms_geodata.find_one({"_id": ObjectId(object_id)})
        row["bad_geometry"] = row.pop("geometry")

        pprint.pprint(row)

        db.ms_geodata.replace_one({"_id": ObjectId(object_id)}, row)


        


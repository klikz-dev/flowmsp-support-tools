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
import geojson
import logging

logging.basicConfig()
logger = logging.getLogger("add_psap")
logger.setLevel(logging.INFO)


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="check for broken links")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("name")
    parser.add_argument("from_email")
    parser.add_argument("--psap-table", default="PSAP")

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")

    client = MongoClient(mongo_uri)
    db = client.FlowMSP

    if args.psap_table not in db.list_collection_names():
        db.create_collection(args.psap_table)
        logger.info("%s: created collection" % args.psap_table)

        # create index

    orec = {
        "_id": str(uuid.uuid4()),
        "name": args.name,
        "from_email": args.from_email,
    }

    r = db[args.psap_table].insert_one(orec)

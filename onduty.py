#!/usr/bin/env python3

import os
import sys
import pprint
import argparse
from pymongo import MongoClient
import boto3
import botocore
import logging

logging.basicConfig()
logger = logging.getLogger("onduty")


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


def get_db(profile):
    session = boto3.session.Session(profile_name=profile)
    mongo_uri = get_parm(session, "mongo_uri")

    client = MongoClient(mongo_uri)
    db = client.FlowMSP

    return db, client


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="copy hydrants")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("slug")
    parser.add_argument("--user")
    parser.add_argument("--update", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--turn-off", action="store_true")
    parser.add_argument("--turn-on", action="store_true")

    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    db, client = get_db(args.profile)
    rows = updated = 0

    query = {}

    if args.turn_off:
        query["isOnDuty"] = True
    else:
        query["isOnDuty"] = False

    if args.user:
        query["email"] = args.user

    logger.info(query)

    src = "%s.User" % args.slug

    for rownum, row in enumerate(db[src].find(query)):
        rows += 1

        if args.turn_off:
            row["isOnDuty"] = False
        else:
            row["isOnDuty"] = True

        pprint.pprint(row)

        if args.update:
            r = db[src].replace_one({"_id": row["_id"]}, row)

            if r:
                updated += 1

            pass

    logger.info("rows=%d" % rows)
    logger.info("updated=%d" % updated)

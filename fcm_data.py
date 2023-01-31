#!/usr/bin/env python3

import os
import sys
import argparse
import logging
import csv
from pymongo import MongoClient
import boto3
import botocore


logging.basicConfig()
logger = logging.getLogger("fcm_data")


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


def get_data(db, collection, key):
    return dict([[row[key], row] for row in db[collection].find()])


def get_customers(db, key):
    return dict([[row[key], row] for row in db["Customer"].find()])
        

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="copy hydrants")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("--slug")
    parser.add_argument("--username")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--platform", choices=["ios", "android"])

    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    db, client = get_db(args.profile)

    customers_byslug = get_customers(db, "slug")
    customers_byid = get_customers(db, "_id")

    passwords_byname = get_data(db, "Password", "username")
    passwords_byid = get_data(db, "Password", "_id")
    
    query = {}

    if args.slug:
        query["customerId"] = customers_byslug[args.slug]["_id"]

    if args.platform:
        query["platform"] = args.platform

    if args.username:
        query["userId"] = passwords_byname[args.username]["_id"]

    rows = 0

    logger.info("query=%s" % query)

    hdr = ["_id", "customerId", "slug", "platform", "registrationToken", "userId", "username"]

    w = csv.DictWriter(sys.stdout, hdr, lineterminator="\n")
    w.writeheader()

    for rownum, row in enumerate(db["FcmData"].find(query)):
        if args.limit and rownum >= args.limit:
            break

        rows += 1
        logger.debug(row)

        slug = customers_byid[row["customerId"]]["slug"]
        username = passwords_byid.get(row["userId"], {}).get("username", "None")

        orec = {
            "_id": row.get("_id", ""),
            "customerId": row.get("customerId", ""),
            "slug": slug,
            "platform": row.get("platform", ""),
            "registrationToken": row.get("registrationToken", ""),
            "userId": row.get("userId", ""),
            "username": username,
        }

        w.writerow(orec)

    logger.info("rows=%d" % rows)
    

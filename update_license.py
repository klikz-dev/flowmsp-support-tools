#!/usr/bin/env python3

import os
import sys
import pprint
import argparse
from pymongo import MongoClient
import boto3
import botocore
import datetime
import logging
from utils import get_db

logging.basicConfig()
logger = logging.getLogger("update_license")
logger.setLevel(logging.INFO)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="update minimumNewHydrantDistance")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("expiration_date")
    parser.add_argument("slug", nargs="+")
    parser.add_argument("--update", action="store_true")
    parser.add_argument("--db-url")

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    client, db = get_db(session, args.db_url)

    query = {"slug": {"$in": args.slug}}

    y, m, d = list(map(int, args.expiration_date.split("-")))
    exp_dt = datetime.datetime(year=y, month=m, day=d)
    slugs = updated = 0

    m = max([len(s) for s in args.slug])

    for row in db.Customer.find(query):
        slugs += 1
        old_dt = row["license"]["expirationTimestamp"]
        row["license"]["expirationTimestamp"] = exp_dt

        logger.info("%-*.*s: old=%s new=%s" %
                    (m, m, row["slug"], old_dt, exp_dt))

        if args.update:
            r = db.Customer.replace_one({"_id": row["_id"]}, row)
            updated += 1

    logger.info("read=%d updated=%d" % (slugs, updated))

            


    

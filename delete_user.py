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
import time
import csv
import re


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


def delete_user(client, db, username, args):
    row = db.Password.find_one({"username": username})

    if row:
        if args.verbose:
            logging.info(row)
    else:
        logging.warning("%s: username not found in Password collection" % username)

    if row:
        collection = "%s.User" % row["customerSlug"]
    else:
        collection = ""

    if collection:
        crow = db[collection].find_one({"email": username})

        if crow:
            if args.verbose:
                logging.info(crow)
        else:
            logging.warning("%s: username not found in %s collection" % (username, collection))

    if args.update is False:
        return 0

    deleted = 0

    r1 = db.Password.delete_one({"username": username})
    deleted += r1.deleted_count

    if args.verbose:
        logging.info("r1.deleted_count=%d" % r1.deleted_count)
        
    if collection:
        r2 = db[collection].delete_one({"email": username})
        deleted += r2.deleted_count

        if args.verbose:
            logging.info("r2.deleted_count=%d" % r2.deleted_count)

    if deleted:
        return 1
    else:
        return 0


def get_db(profile):
    session = boto3.session.Session(profile_name=profile)
    mongo_uri = get_parm(session, "mongo_uri")
    client = MongoClient(mongo_uri)
    db = client.FlowMSP

    return client, db
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="delete user accounts, reads from stdin unless usernames provided on command line")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("usernames", nargs="*")
    parser.add_argument("--update", action="store_true")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--verbose", action="store_true")

    args = parser.parse_args()
    client, db = get_db(args.profile)

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    rows = 0

    if args.usernames:
        for u in args.usernames:
            rows += delete_user(client, db, u, args)
    else:
        logging.info("reading usernames from stdin")

        for u in sys.stdin:
            rows += delete_user(client, db, u.strip(), args)

    logging.info("deleted=%d" % rows)




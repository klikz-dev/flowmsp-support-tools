#!/usr/bin/env python3

import os
import sys
import pprint
import argparse
import re
from pymongo import MongoClient
import boto3
import botocore
import logging

logging.basicConfig()
logger = logging.getLogger("switch_slug")
logger.setLevel(logging.INFO)


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


def get_db(url):
    client = MongoClient(url)
    db = client.FlowMSP
    
    return client, db


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="check for broken links")
    parser.add_argument("username")
    parser.add_argument("slug")
    parser.add_argument("--role", choices=["ADMIN", "PLANNER", "USER"])
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--db-url")
    parser.add_argument("--profile", default="flowmsp-prod")

    args = parser.parse_args()

    # if db_url is provided, use it
    if args.db_url:
        client, db = get_db(args.db_url)
    else:
        session = boto3.session.Session(profile_name=args.profile)
        url = get_parm(session, "MONGO_URI")
        client, db = get_db(url)

    user = db.Password.find_one({"username": args.username})

    if not user:
        user = {'_id': '03f03851-1fc1-4e32-b191-3a813a9dfd67',
                'customerId': '513951b7-a94b-402d-b63c-d2494f283ad0',
                'customerSlug': 'doug',
                'password': '$2a$10$mi7Jp5dEatNbCrJhfhoY1uuW1mzdXFvcwt7M1G0rutFCobW3HLsFi',
                'username': 'doug@dhware.com'}
        db.Password.insert_one(user)

    if not user:
        logger.error("%s: user not found" % args.username)
        raise SystemExit(1)

    customer = db.Customer.find_one({"slug": args.slug})

    if not customer:
        logger.error("%s: slug not found" % args.slug)
        raise SystemExit(1)

    cuser = db["%s.User" % user["customerSlug"]].find_one({"email": args.username})

    if not cuser:
        cuser = {'_id': '03f03851-1fc1-4e32-b191-3a813a9dfd67',
                 'customerRef': {'customerId': '513951b7-a94b-402d-b63c-d2494f283ad0',
                                 'customerName': 'Doug',
                                 'customerSlug': 'doug'},
                 'email': 'doug@dhware.com',
                 'firstName': 'Doug',
                 'lastName': 'Harvey',
                 'role': 'ADMIN',
                 'uiConfig': {}}

    if not cuser:
        logger.error("%s: cuser not found" % args.username)
        raise SystemExit(1)

    if args.verbose:
        print("before")
        pprint.pprint(user)
        print()

    if user["customerSlug"] == args.slug:
        logger.error("%s: already set to %s" % (args.username, args.slug))
        raise SystemExit(0)

    user["customerSlug"] = args.slug
    user["customerId"] = customer["_id"]
    db.Password.replace_one({"_id": user["_id"]}, user)

    if args.verbose:
        print("after")
        pprint.pprint(user)
        print()

    #------------------------------------------------------------
    # move customer-specific entry to new customer
    #------------------------------------------------------------
    if args.verbose:
        print("before")
        pprint.pprint(cuser)
        print()

    oslug = cuser["customerRef"]["customerSlug"]

    cuser["customerRef"]["customerId"] = customer["_id"]
    cuser["customerRef"]["customerName"] = customer["name"]
    cuser["customerRef"]["customerSlug"] = customer["slug"]

    if args.role:
        cuser["role"] = args.role

    if args.verbose:
        print("after")
        pprint.pprint(cuser)
        print()

    # insert new record
    db["%s.User" % args.slug].insert_one(cuser)

    # remove old record
    db["%s.User" % oslug].delete_one({"_id": cuser["_id"]})

    print("moved %s from %s to %s" % (args.username, oslug, args.slug))

                                    

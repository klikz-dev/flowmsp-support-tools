#!/usr/bin/env python3

import os
import sys
import pprint
import argparse
import re
import logging
from pymongo import MongoClient
import boto3
import botocore
from utils import get_db

logging.basicConfig()
logger = logging.getLogger("users")
logger.setLevel(logging.INFO)


def check_usernames(db):
    for rownum, row in enumerate(db.Password.find()):
        if not re.match(r"^[a-z0-9@.\-_]+$", row["username"], re.IGNORECASE):
            pprint.pprint(row)

    print("rows=%d" % rownum)

    return


def print_password_row(row, args):
    if args.multi_line:
        pprint.pprint(row)
    else:
        print(row["_id"],
              row["customerId"],
              row["customerSlug"],
              row["password"],
              row["username"])

    return


def print_cuser_row(row):
    pprint.pprint(row)
    return

    print(row.get("email"),
          row.get("firstName"),
          row.get("lastName"),
          row.get("role"))

    return

def process_cuser_table(db, collection, username, role, find_orphans, customers):
    q = {}

    if role:
        q["role"] = role

    for row in db[collection].find(q):
        if username:
            if username.lower() in row.get("email").lower():
                print_cuser_row(row)
        elif find_orphans:
            if row["customerRef"]["customerSlug"] not in customers:
                # print_cuser_row(row)
                print("collection", collection)
                pprint.pprint(row)

        else:
            print_cuser_row(row)

    return


def load_customers(db):
    customers = {}

    for row in db.Customer.find():
        if "slug" not in row:
            logger.warning("no slug", row)
            continue

        customers[row["slug"]] = row

    return customers


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="check for broken links")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("--slug", help="customer name, aka slug")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--multi-line", action="store_true")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--username")
    parser.add_argument("--include-customer-tables", action="store_true")
    parser.add_argument("--find-orphans", action="store_true")
    parser.add_argument("--check-consistency", action="store_true")
    parser.add_argument("--role")
    parser.add_argument("--db-url")

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    client, db = get_db(session, args.db_url)

    q = {}

    customers = load_customers(db)

    if args.slug:
        q["customerSlug"] = args.slug

    for row in db.Password.find(q):
        if args.username:
            if args.username.lower() in row["username"].lower():
                print_password_row(row, args)
        elif args.find_orphans:
            if row["customerSlug"] not in customers:
                print_password_row(row, args)
        else:
            print_password_row(row, args)


    if args.include_customer_tables is False:
        raise SystemExit(0)

    print()
    print("looking at customer tables")

    if args.slug:
        process_cuser_table(db, "%s.User" % args.slug, args.username, args.role, args.find_orphans, customers)
    else:
        for row in db.list_collections():
            if row["name"].endswith(".User"):
                process_cuser_table(db, row["name"], args.username, args.role, args.find_orphans, customers)



#!/usr/bin/env python3

import os
import sys
import pprint
import argparse
import csv
from pymongo import MongoClient
import boto3
import botocore


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


def get_customer(db, slug):
    return db.Customer.find_one({"slug": slug})


def get_db(profile):
    session = boto3.session.Session(profile_name=profile)
    mongo_uri = get_parm(session, "mongo_uri")
    client = MongoClient(mongo_uri)

    return client, client.FlowMSP
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="lowercase usernames")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--slug")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--verbose", action="store_true")

    args = parser.parse_args()
    client, db = get_db(args.profile)
    c = get_customer(db, args.slug)

    rows = notfound = updated = skipped = 0

    q = {}

    if args.slug:
        q["customerSlug"] = args.slug

    for rownum, row in enumerate(db.Password.find(q)):
        # already lowercase
        if row.get("username", "") == row.get("username", "").lower():
            continue

        rows += 1

        if args.limit and rows >= args.limit:
            break

        username = row["username"]

        if args.verbose:
            print(row)
    
        utbl = db["%s.User" % row["customerSlug"]]
        user = utbl.find_one({"email": row["username"]})

        if args.verbose:
            print(user)

        if user:
            user["email"] = user["email"].lower()

        with client.start_session() as s:
            s.start_transaction()

            # replace row in customer-user table
            if user:
                if args.dry_run is False:
                    utbl.replace_one({"_id": user["_id"]}, user)

            # fix in DebugInfo
            if args.verbose:
                print("DebugInfo")

            debug_records = 0

            for r in db.DebugInfo.find({"Details.userName": row["username"]}):
                if args.verbose:
                    print(r)
                r["Details"]["userName"] = r["Details"]["userName"].lower()
                debug_records += 1

                if args.dry_run is False:
                    db.DebugInfo.replace_one({"_id": r["_id"]}, r)

            row["username"] = row["username"].lower()

            if args.dry_run is False:
                db.Password.replace_one({"_id": row["_id"]}, row)

            s.commit_transaction()

        updated += 1

        if args.verbose:
            print("------------------------------------------------------------")
            print()
        else:
            print(rownum, rows, row["customerSlug"], username, "debug_records=%d" % debug_records)
        
        continue

    # fix in AuthLog
    # entries are stored in a strange way here
    # maybe do this separately

    print("rows=%d skipped=%d notfound=%d updated=%d" %
          (rows, skipped, notfound, updated), file=sys.stderr)

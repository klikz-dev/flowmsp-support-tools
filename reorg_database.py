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


def get_collections(db, name):
    for c in db.list_collection_names():
        if "dhware,inc" in c:
            continue
    
        if c.endswith(".%s" % name):
            yield c

    return


def copy_table(db, table, args):
    if table in db.list_collection_names():
        db.drop_collection(table)
        print("%s: dropped" % table)

    db.create_collection(table)
    print("%s: created" % table)
    tables = total_rows = 0

    for c in get_collections(db, table):
        batch = []
        rows = 0

        for r in db[c].find():
            rows += 1

            if args.limit and rows > args.limit:
                break

            batch.append(r)

            if len(batch) >= 500:
                if args.dry_run is False:
                    db[table].insert_many(batch)
                batch = []

        if batch:
            if args.dry_run is False:
                db[table].insert_many(batch)

        print("%s: inserted %d rows from %s" % (table, rows, c))
        total_rows += rows
        tables += 1
        
    print("%s: inserted %d rows from %d tables" % (table, total_rows, tables))

    return


def get_db(profile):
    session = boto3.session.Session(profile_name=profile)
    mongo_uri = get_parm(session, "mongo_uri")

    client = MongoClient(mongo_uri)
    db = client.FlowMSP

    return db, client


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="combine customer tables")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("tables", nargs="*",
                        default=["Hydrant", "User", "Location", "MsgReceiver"])
    parser.add_argument("--limit", type=int)
    parser.add_argument("--dry-run", action="store_true")


    args = parser.parse_args()
    db, client = get_db(args.profile)

    for t in args.tables:
        copy_table(db, t, args)

    # create indexes

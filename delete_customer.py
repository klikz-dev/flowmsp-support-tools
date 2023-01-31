#!/usr/bin/env python3

import os
import sys
import pprint
import argparse
import csv
from pymongo import MongoClient
import boto3
import botocore
import pymongo
from bson.json_util import dumps
import time
import logging
from utils import get_db

logging.basicConfig()
logger = logging.getLogger("delete_customer")
logger.setLevel(logging.INFO)


def get_customer(db, name):
    return db.Customer.find_one({"slug": name})


def dump_rows(db, name, args, slug, query={}):
    rows = [row for row in db[name].find(query)]

    data = dumps(rows, indent=4)

    # should use mktemp or similar
    fp = open("%s.json" % name, "w")
    print(data, file=fp)
    fp.close()

    ymd = time.strftime("%Y%m%d")
    dst = "{bucket}/{slug}/backup/{name}_{ymd}.json".format(
        bucket=args.bucket,
        slug=slug,
        name=name,
        ymd=ymd
    )

    #print("dst", dst)
    cmd = "aws --profile {profile} s3 cp --only-show-errors {name} {dst}".format(
        profile=args.profile,
        name=fp.name,
        dst=dst
    )
    #print("cmd", cmd)
    rc = os.system(cmd)
    os.remove(fp.name)

    return

def delete_from(db, name, key, value, slug, args):
    query = {key: value}
    rows = db[name].count_documents(query)
    logger.info("%d rows in %s for %s" % (rows, name, query))

    if args.skip_backup is False:
        dump_rows(db, name, args, slug, query)

    if args.update is False:
        return

    result = db[name].delete_many({key: value})
    logger.info("deleted %d rows" % result.deleted_count)

    return
    

def drop_collection(db, name, args):
    rows = db[name].count_documents({})
    slug = name.split(".")[0]

    print("%d rows in %s" % (rows, name))

    if args.skip_backup is False:
        dump_rows(db, name, args, slug)

    if args.update is False:
        return

    db.drop_collection(c)

    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="check for broken links")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("slug", nargs='+')
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--bucket", default="s3://flowmsp-client-data")
    parser.add_argument("--skip-backup", action="store_true")
    parser.add_argument("--update", action="store_true")

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    client, db = get_db(session)

    for slug in args.slug:
        cust = get_customer(db, slug)

        if not cust:
            logger.warning("%s: slug not found" % slug)

        if args.verbose:
            logger.info(cust)
        
        for coll in "Hydrant", "Location", "User", "MsgReceiver":
            c = "%s.%s" % (slug, coll)
          
            if c in db.list_collection_names():
                drop_collection(db, c, args)

        if cust:
            delete_from(db, "Partners", "customerId", cust["_id"], slug, args)
            delete_from(db, "Partners", "partnerId", cust["_id"], slug, args)

        delete_from(db, "Password", "customerSlug", slug, slug, args)
        delete_from(db, "Customer", "slug", slug, slug, args)

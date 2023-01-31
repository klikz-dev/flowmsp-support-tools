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
import logging
from utils import get_db


logging.basicConfig()
logger = logging.getLogger("add_psap")
logger.setLevel(logging.INFO)


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


def get_customers(db):
    tbl = {}

    for row in db.Customer.find():
        if "slug" not in row:
            continue

        slug = row["slug"]
        id = row["_id"]

        tbl[slug] = id

    return tbl


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="check for broken links")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("psap_registry_id")
    parser.add_argument("--file")
    parser.add_argument("--table", default="PsapUnitCustomer")
    parser.add_argument("--db-url")
    parser.add_argument("--replace", action="store_true")

    args = parser.parse_args()
    session = boto3.session.Session(profile_name=args.profile)

    client, db = get_db(session, args.db_url)

    if args.table not in db.list_collection_names():
        db.create_collection(args.table)
        logger.info("%s: created collection" % args.table)
        # create index

    inserts = 0
    customers = get_customers(db)

    psap = db.PSAP.find_one({"registryId": args.psap_registry_id})

    if psap is None:
        logger.error("%s: PSAP not found" % args.psap_name)
        raise SystemExit(1)

    logger.info(psap)

    if args.replace:
        r = db[args.table].delete_many({"psapId": psap["_id"]})
        logger.info("deleted %d rows" % r.deleted_count)

    if args.file:
        r = csv.DictReader(open(args.file))

        for rec in r:
            rec["_id"] = str(uuid.uuid4())
            rec["psapId"] = psap["_id"]
    
            if rec["slug"]:
                rec["customerId"] = customers.get(rec["slug"])
            
            #print(rec)
            res = db[args.table].insert_one(rec)
            inserts += 1
    
        logger.info("inserted %d rows" % inserts)
    else:
        recs = [rec for rec in db[args.table].find({"psapId": psap["_id"]})]

        # get columns
        fields = set()

        for rec in recs:
            for k in rec:
                fields.add(k)

        hdr = list(fields)

        w = csv.DictWriter(sys.stdout, hdr, lineterminator="\n")
        w.writeheader()

        for rec in recs:
            w.writerow(rec)




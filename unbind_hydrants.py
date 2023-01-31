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
import uuid


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="remove hydrants from preplan")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("slug")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--save-collection", default="hydrant_mappings")
    parser.add_argument("--update", action="store_true")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--delete-existing-mappings", action="store_true")

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")
    client = MongoClient(mongo_uri)
    db = client.FlowMSP
    coll = "%s.Location" % args.slug
    rows = updated = 0

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    logging.debug("coll=%s" % coll)

    if args.save_collection not in db.list_collection_names():
        db.create_collection(args.save_collection)

        logging.info("%s: created collection" % args.save_collection)

    if args.delete_existing_mappings:
        r = db[args.save_collection].delete_many({"slug": args.slug})
        logging.info("deleted %d rows" % r.deleted_count)

    if "time_ns" in dir(time):
        batchNo = time.time_ns()
    else:
        batchNo = int(time.time())

    logging.info("batchNo=%s" % batchNo)

    tstamp = datetime.datetime.now()
    logging.info("tstamp=%s" % tstamp)

    for rownum, row in enumerate(db[coll].find()):
        if args.limit and rownum >= args.limit:
            break

        rows += 1

        if len(row.get("hydrants", [])) > 0:
            id = row["_id"]

            hydrants = row.pop("hydrants")
            save = {
                "_id": str(uuid.uuid4()),
                "location_id": row["_id"],
                "slug": args.slug,
                "hydrants": hydrants,
                "batchNo": batchNo,
                "tstamp": tstamp 
            }

            logging.debug(save)

            if args.update:
                r = db[args.save_collection].insert_one(save)
                
                r = db[coll].replace_one({"_id": id}, row)
                updated += 1

    logging.info("rows=%d updated=%d" % (rows, updated))



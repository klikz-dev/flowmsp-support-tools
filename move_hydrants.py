#!/usr/bin/env python3

import os
import sys
import pprint
import argparse
from pymongo import MongoClient
import boto3
import botocore
import logging

logging.basicConfig()
logger = logging.getLogger("move_hydrants")


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


def print_hydrant(label, h):
    if "createdOn" in h:
        tstamp = h["createdOn"].strftime("%Y-%m-%d %H:%M:%S")
    else:
        tstamp = ""

    logger.debug(h)

    x = [label,
         h.get("_id", ""),
         h.get("customerSlug", ""),
         tstamp,
         h.get("lonLat", ""),
         h.get("flow", ""),
         h.get("streetAddress", ""),
         h.get("notes", "")]

    y = list(map(str, x))

    return " ".join(y)


def move_hydrant(client, src, dst, row, customer, update):
    inserted = deleted = 0

    with client.start_session() as s:
        s.start_transaction()

        # remove hydrant from src
        if update:
            r = src.delete_one({"_id": row["_id"]})
            deleted += r.deleted_count

        logger.debug(print_hydrant("BEFORE", row))

        row["customerId"] = customer["_id"]
        row["customerSlug"] = customer["slug"]

        logger.debug(print_hydrant("AFTER", row))

        if update:
            r = dst.insert_one(row)

            if r:
                inserted += 1

        s.commit_transaction()

    return inserted, deleted


def get_customer(db, slug):
    return db.Customer.find_one({"slug": slug})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="copy hydrants")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("src")
    parser.add_argument("dst")
    parser.add_argument("--batch-no", type=int)
    parser.add_argument("--id")
    parser.add_argument("--update", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--limit", type=int)

    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    db, client = get_db(args.profile)
    rows = inserted = deleted = 0

    customer = get_customer(db, args.dst)
    src = "%s.Hydrant" % args.src
    dst = "%s.Hydrant" % args.dst

    query = {}

    #if args.batch_no:
    query["batchNo"] = args.batch_no

    if args.id:
        query["_id"] = args.id

    for rownum, row in enumerate(db[src].find(query)):
        if args.limit and rownum >= args.limit:
            break

        rows += 1
        logger.debug(row)

        if args.update:
            i, d = move_hydrant(client, db[src], db[dst], row, customer, args.update)
            inserted += i
            deleted += d

    logger.info("rows=%d" % rows)
    logger.info("inserted=%d" % inserted)
    logger.info("deleted=%d" % deleted)

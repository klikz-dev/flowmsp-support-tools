#!/usr/bin/env python3

import sys
import argparse
import pickle
from pymongo import MongoClient
import boto3
import botocore
import logging
import time


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


def get_date():
    return time.strftime("%Y%m%d")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="check for broken links")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("slug")
    parser.add_argument("batch_no", type=int)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--update", action="store_true")
    parser.add_argument("--save-file", default="saved_locations_%s.pickle" % get_date())

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")
    client = MongoClient(mongo_uri)

    logging.basicConfig(level=logging.INFO)

    db = client.FlowMSP

    coll = "%s.Location" % args.slug

    query = {"batchNo": args.batch_no}
    output = []
    found = deleted = skipped = 0

    fp = open(args.save_file, "wb")

    for rownum, row in enumerate(db[coll].find(query)):
        if args.limit and rownum >= args.limit:
            break

        if len(row.get("images", [])) > 0:
            logging.info("skipping: %s %s %s %d" % \
                         (row["_id"],
                         row.get("name", ""),
                         row.get("address", {}).get("address1", ""),
                         len(row.get("images", []))))
            skipped += 1
            continue

        if "modifiedOn" in row and row["modifiedOn"] != row["createdOn"]:
            logging.info("skipping: %s %s %s %d" % \
                         (row["_id"],
                         row.get("name", ""),
                         row.get("address", {}).get("address1", ""),
                         len(row.get("images", []))))
            skipped += 1
            continue

        found += 1
        output.append(row)

        if args.update:
            result = db[coll].delete_one({"_id": row["_id"]})
            deleted += result.deleted_count

    logging.info("found=%d" % found)
    logging.info("skipped=%d" % skipped)
    logging.info("deleted=%d" % deleted)

    pickle.dump(output, fp)
    fp.close()


    

#!/usr/bin/env python3

import os
import sys
import argparse
from pymongo import MongoClient
import boto3
import botocore
import logging
import csv


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


def get_db(profile):
    session = boto3.session.Session(profile_name=profile)
    mongo_uri = get_parm(session, "mongo_uri")
    client = MongoClient(mongo_uri)
    db = client.FlowMSP

    return client, db
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="hydrant report")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("slug")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--limit", type=int)

    args = parser.parse_args()
    client, db = get_db(args.profile)

    logging.basicConfig(level=logging.INFO)

    rows = 0

    coll = "%s.Hydrant" % args.slug

    hdr = [
        "_id", "batch_no", "created_by", "created_on", "customer_id",
        "customer_slug", "dry_hydrant", "hydrant_id", "flow", "flow_range",
        "in_service", "lat", "lon", "modified_by", "modified_on", "notes",
        "out_service_date", "size", "street_address",
        ]

    rows = 0

    w = csv.DictWriter(sys.stdout, hdr, lineterminator="\n")
    w.writeheader()

    for rownum, row in enumerate(db[coll].find()):
        if args.limit and rownum >= args.limit:
            break

        rows += 1

        orec = {
            "_id": row["_id"],
            "batch_no": row.get("batchNo", ""),
            "created_by": row.get("createdBy", ""),
            "created_on": row.get("createdOn", ""),
            "customer_id": row.get("customerId", ""),
            "customer_slug": row.get("customerSlug", ""),
            "dry_hydrant": row.get("dryHydrant", ""),
            "hydrant_id": row.get("hydrantId", ""),
            "flow": row.get("flow", ""),
            "flow_range": row.get("flowRange", {}).get("pinColor", ""),
            "in_service": row.get("inService", ""),
            "lat": row.get("lonLat", {}).get("coordinates", [0.0, 0.0])[1],
            "lon": row.get("lonLat", {}).get("coordinates", [0.0, 0.0])[0],
            "modified_by": row.get("modifiedBy", ""),
            "modified_on": row.get("modifiedOn", ""),
            "notes": row.get("notes", ""),
            "out_service_date": row.get("outServiceDate", ""),
            "size": row.get("size", ""),
            "street_address": row.get("streetAddress", "").replace("\r", "")
        }

        w.writerow(orec)

    logging.info("rows=%d" % rows)




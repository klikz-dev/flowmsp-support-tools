#!/usr/bin/env python3

import os
import sys
import pprint
import argparse
import datetime
import boto3
import botocore
import csv
from utils import get_db


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="list slugs")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("--limit", type=int)
    parser.add_argument("--db-url")

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    client, db = get_db(session, args.db_url)

    hdr = ["slug", "name", "address1", "city", "state", "license_type", "license_created", "license_expires"]
    w = csv.DictWriter(sys.stdout, hdr, lineterminator="\n", delimiter="\t")
    w.writeheader()

    for rownum, row in enumerate(db.Customer.find()):
        if args.limit and rownum >= args.limit:
            break

        orec = {
            "slug": row["slug"],
            "name": row.get("name", ""),
            "address1": row.get("address", {}).get("address1"),
            "city": row.get("address", {}).get("city"),
            "state": row.get("address", {}).get("state"),
            "license_type": row.get("license", {}).get("licenseType", ""),
            "license_created": row.get("license", {}).get("creationTimestamp", ""),
            "license_expires": row.get("license", {}).get("expirationTimestamp", ""),
        }

        w.writerow(orec)


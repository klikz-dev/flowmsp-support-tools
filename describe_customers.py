#!/usr/bin/env python3

import sys
import argparse
import json
import pprint
from pymongo import MongoClient
import boto3
import botocore
import logging
from utils import get_db

logging.basicConfig()
logger = logging.getLogger("describe_customers")
logger.setLevel(logging.INFO)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="check for broken links")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("--query")
    parser.add_argument("--limit", type=int)

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    client, db = get_db(session)

    results = []

    query = {}

    if args.query:
        query = json.loads(args.query)

    results = []

    for cnum, c in enumerate(db.Customer.find(query)):
        if args.limit and cnum >= args.limit:
            break

        pprint.pprint(c)
        results.append(c)


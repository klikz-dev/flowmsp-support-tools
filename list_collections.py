#!/usr/bin/env python3

import os
import sys
import pprint
import argparse
from pymongo import MongoClient
import boto3
import botocore
import logging
import re
from utils import get_db


logging.basicConfig()
logger = logging.getLogger("list_collections")
logger.setLevel(logging.INFO)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="list collections")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("--limit", type=int)
    parser.add_argument("--exclude-user-tables", action="store_true")

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    client, db = get_db(session)

    user_tables = r".*\.(User|Hydrant|Location|MsgReceiver)$"

    for cnum, c in enumerate(db.list_collections()):
        if args.limit and cnum >= args.limit:
            break

        if args.exclude_user_tables and re.match(user_tables, c["name"]):
            continue

        print(c["name"])

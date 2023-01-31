#!/usr/bin/env python3

import os
import sys
import pprint
import argparse
import datetime
from pymongo import MongoClient
import boto3
import botocore
from utils import get_db

dispatch_keys = ["emailFormat", "emailGateway", "SFTP_userid"]

def reset(row):
    for k in dispatch_keys:
        if k in row:
            row.pop(k)

    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="set dispatcher fields")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("registry_id")
    parser.add_argument("--email-format")
    parser.add_argument("--email-gateway")
    parser.add_argument("--sftp-userid")
    parser.add_argument("--update", action="store_true")
    parser.add_argument("--reset", action="store_true")
    parser.add_argument("--db-url")

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    client, db = get_db(session, args.db_url)

    row = db.PSAP.find_one({"registryId": args.registry_id})

    if args.reset:
        reset(row)

    if args.email_format:
        row["emailFormat"] = args.email_format

    if args.email_gateway:
        row["emailGateway"] = args.email_gateway
        
    if args.sftp_userid:
        row["SFTP_userid"] = args.sftp_userid

    pprint.pprint(dict([[k, v] for k, v in row.items() if k in dispatch_keys]))

    if args.update:
        r = db.PSAP.replace_one({"_id": row["_id"]}, row)
        print("updated", r)
    else:
        print("nothing updated")



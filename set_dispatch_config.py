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

dispatch_keys = ["emailFormat", "emailGateway", "emailSignature", "emailSignatureLocation",
                 "from", "subjectContains", "subjectNotContains", "toContains", "toNotContains",
                 "bodyContains", "bodyNotContains", "smsFormat", "smsNumber", "emailFormat",
                 "emailGateway", "emailSignature", "emailSignatureLocation",
                 "subjectContains", "subjectNotContains", "toContains", "demoDispatch",
                 "toNotContains", "bodyContains", "bodyNotContains", "smsFormat", "smsNumber",
                 "SFTP_userid",
                 ]

def reset(row):
    for k in dispatch_keys:
        if k in row:
            row.pop(k)

    return


def clone_slug(db, row, slug):
    clone = db.Customer.find_one({"slug": slug})

    if not clone:
        print(f"{slug}: not found", file=sys.stderr)
        return

    reset(row)
    
    for k in dispatch_keys:
        if k in clone:
            row[k] = clone[k]

    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="set dispatcher fields")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("slug")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--email-format")
    parser.add_argument("--email-gateway")
    parser.add_argument("--email-signature")
    parser.add_argument("--email-signature-location")
    parser.add_argument("--from-contains")
    parser.add_argument("--from-not-contains")
    parser.add_argument("--subject-contains")
    parser.add_argument("--subject-not-contains")
    parser.add_argument("--to-contains")
    parser.add_argument("--to-not-contains")
    parser.add_argument("--body-contains")
    parser.add_argument("--body-not-contains")
    parser.add_argument("--sms-format")
    parser.add_argument("--sms-number")
    parser.add_argument("--update", action="store_true")
    parser.add_argument("--show-current", action="store_true")
    parser.add_argument("--reset", action="store_true")
    parser.add_argument("--demo-dispatch", action="store_true")
    parser.add_argument("--demo-days", type=int, default=4)
    parser.add_argument("--no-demo-dispatch", action="store_true")
    parser.add_argument("--clone-slug")
    parser.add_argument("--sftp-userid")
    parser.add_argument("--db-url")

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    client, db = get_db(session, args.db_url)

    row = db.Customer.find_one({"slug": args.slug})

    if args.reset:
        reset(row)

    if args.clone_slug:
        clone_slug(db, row, args.clone_slug)

    if args.show_current:
        pprint.pprint(row)
        raise SystemExit(0)

    if args.demo_dispatch:
        row["demoDispatch"] = datetime.datetime.utcnow() + datetime.timedelta(days=args.demo_days)
        row["emailFormat"] = "sandbox"
        row["emailGateway"] = "@flowmsp.com"
        row["toContains"] = "alerts+%s@flowmsp.com" % row["slug"]

    if args.no_demo_dispatch:
        row["demoDispatch"] = datetime.datetime.utcnow()
        row.pop("emailFormat", "")
        row.pop("emailGateway", "")

    if args.email_format:
        row["emailFormat"] = args.email_format

    if args.email_gateway:
        row["emailGateway"] = args.email_gateway
        
    if args.email_signature:
        row["emailSignature"] = args.email_signature
        
    if args.email_signature_location:
        row["emailSignatureLocation"] = args.email_signature_location

    if args.from_contains:
        row["from"] = args.from_contains

    if args.subject_contains:
        row["subjectContains"] = args.subject_contains

    if args.subject_not_contains:
        row["subjectNotContains"] = args.subject_not_contains

    if args.to_contains:
        row["toContains"] = args.to_contains

    if args.to_not_contains:
        row["toNotContains"] = args.to_not_contains

    # parser.add_argument("--from-not-contains")

    if args.body_contains:
        row["bodyContains"] = args.body_contains

    if args.body_not_contains:
        row["bodyNotContains"] = args.body_not_contains

    if args.sms_format:
        row["smsFormat"] = args.sms_format

    if args.sms_number:
        row["smsNumber"] = args.sms_number

    if args.sftp_userid:
        row["SFTP_userid"] = args.sftp_userid

    pprint.pprint(dict([[k, v] for k, v in row.items() if k in dispatch_keys]))
    #pprint.pprint(row)

    if args.update:
        r = db.Customer.replace_one({"_id": row["_id"]}, row)
        print("updated", r)
    else:
        print("nothing updated")



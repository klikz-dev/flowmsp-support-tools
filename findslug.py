#!/usr/bin/env python3

import os
import sys
import pprint
import argparse
import pprint
import csv
import datetime
import json
from pymongo import MongoClient
import boto3
import botocore


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


def get_field(c, path):
    keys = path.split(".")
    d = c

    for k in keys:
        d = d.get(k)

    while d and keys and type(d) is dict:
        k = keys.pop(0)
        d = d.get(k)

    return d
    

def get_admin_user(db, slug):
    return db["%s.User" % slug].find_one({"role": "ADMIN"}) or {}


def print_customer_info(w, c, db, args):
    slug = c["slug"]

    num_users = db["%s.User" % slug].count_documents({})
    num_locations = db["%s.Location" % slug].count_documents({})
    num_dispatches = db["%s.MsgReceiver" % slug].count_documents({})
    num_hydrants = db["%s.Hydrant" % slug].count_documents({})
    num_images = 0
    last_modified = []
    admin = get_admin_user(db, slug)

    for loc in db["%s.Location" % c["slug"]].find():
        if "images" in loc:
            num_images += len(loc["images"])

        if "modifiedOn" in loc:
            last_modified.append(loc["modifiedOn"])

    max_last_modified = ""

    if last_modified:
        max_last_modified = max(last_modified)

    if "license" in c:
        created = c["license"].get("creationTimestamp")
        expires = c["license"].get("expirationTimestamp")
    else:
        created = expires = ""

    if args.multi_line:
        print("------------------------------------------------------------")
        pprint.pprint(c)

        for row in db["%s.User" % slug].find({"role": "ADMIN"}):
            print(row["email"], row["firstName"], row["lastName"], row["role"])
    else:
        if "address" in c:
            addr1 = c["address"].get("address1")


        w.writerow([
            c["_id"],
            c["slug"],
            c["name"],
            c["license"]["licenseType"],
            created,
            expires,
            get_field(c, "address.address1"),
            get_field(c, "address.city"),
            get_field(c, "address.state"),
            get_field(c, "address.zip"),
            num_locations,
            num_hydrants,
            num_dispatches,
            max_last_modified,
            num_images,
            num_users,
            c.get("emailFormat", ""),
            admin.get("email")
            ])

    return


def check_customer(cust, slug):
    # pprint.pprint(cust)

    if "license" in cust:
        for k in "expirationTimestamp", "creationTimestamp":
            if k in cust["license"]:
                cust["license"][k] = str(cust["license"][k])

    # turn into a string and search everything
    s = json.dumps(cust, indent=4).lower()

    if slug.lower() in s:
        return True
    else:
        return False

    if slug in cust["slug"]:
        return True

    elif slug in cust["name"].lower():
        return True

    elif "address" in cust and slug in cust["address"].get("address1", "").lower():
        return True

    elif "address" in cust and slug in cust["address"].get("zipcode", "").lower():
        return True

    return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="check for broken links")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("customer_name", help="customer name, aka slug",
                        nargs='*')
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--multi-line", action="store_true")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--exact-match", action="store_true")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--created", help="creationTime >= date")
    parser.add_argument("--sms", action="store_true")

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")
    image_uri = get_parm(session, "image_uri")

    client = MongoClient(mongo_uri)

    db = client.FlowMSP

    results = []

    query = {}

    if args.created:
        query["license.creationTimestamp"] = {"$gte": datetime.datetime.strptime(args.created,"%Y-%m-%d")}

    if args.sms:
        # query["smsNumber"] = {"$ne": ""}

        for row in db.Customer.find(query):
            smsNumber = row.get("smsNumber", "")

            if smsNumber:
                results.append(row)

    elif not args.customer_name:
        results = [row for row in db.Customer.find(query)]

    elif args.exact_match:
        for slug in args.customer_name:
            c = db.Customer.find_one({"slug": slug})

            if c:
                results.append(c)
    else:
        for c in db.Customer.find(query):
            for slug in args.customer_name:
                if check_customer(c, slug):
                    results.append(c)
                    break

    if args.multi_line is False:
        w = csv.writer(sys.stdout, lineterminator="\n")

        w.writerow([
            "_id",
            "slug",
            "name",
            "licenseType",
            "created",
            "expires",
            "address1", "city", "state", "zip",
            "num_locations",
            "num_hydrants",
            "num_dispatches",
            "max_last_modified",
            "num_images",
            "num_users",
            "email_format",
            "admin_user"])

    else:
        w = None

    for i, c in enumerate(results):
        if args.limit and i >= args.limit:
            break

        print_customer_info(w, c, db, args)

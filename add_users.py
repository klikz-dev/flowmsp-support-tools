#!/usr/bin/env python3

import sys
import pprint
import time
import argparse
import requests
import boto3
import csv
import getpass
from pymongo import MongoClient
import logging

logging.basicConfig()
logger = logging.getLogger("add_users")
logger.setLevel(logging.INFO)


def get_link(links, rel):
    for link in links:
        if link["rel"] == rel:
            return link["href"]


def login(hostname, userid, pw):
    # url = "http://{hostname}/api/auth/token".format(hostname=hostname)
    url = "{hostname}/api/auth/token".format(hostname=hostname)

    r = requests.post(url, auth=(userid, pw))

    if r.ok:
        auth = r.json()
    else:
        print(r.status_code, file=sys.stderr)
        print(r.text, file=sys.stderr)
        raise SystemExit(1)

    headers = {"Authorization": "{token_type} {access_token}".format(
        token_type=auth["tokenType"],
        access_token=auth["accessToken"])
    }

    return auth, headers


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="recalculate pre-plans")
    parser.add_argument("userid")
    parser.add_argument("roster")
    parser.add_argument("--password")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--api-server", default="https://app.flowmsp.com")
    parser.add_argument("--email", default="email")
    parser.add_argument("--role", default="role")

    args = parser.parse_args()

    password = args.password or getpass.getpass()

    auth, headers = login(args.api_server, args.userid, password)

    customer_url = get_link(auth["links"], "customer")

    r = requests.get(customer_url, headers=headers)

    if r.ok:
        customer = r.json()

    print("Name: %s" % customer["name"])
    print("Slug: %s" % customer["slug"])
    print("Addr: %s" % customer["address"]["address1"])
    print("City: %s, %s  %s" %
          (customer["address"]["city"],
          customer["address"]["state"],
          customer["address"]["zip"]))
    print()

    slug = customer["slug"]
    url = f"{args.api_server}/api/{slug}/user/createMain"

    fp = open(args.roster)
    r = csv.DictReader(fp)
    logger.info(r.fieldnames)
    
    for rec in r:
        user = {"email": rec[args.email].lower().strip(), "role": rec[args.role].strip().upper()}

        print(user)

        r = requests.post(url, headers=headers, json=user)
        print(r.ok)


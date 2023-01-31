#!/usr/bin/env python3

import sys
import pprint
import time
import argparse
import requests
import boto3
import csv
import getpass
import logging
from pymongo import MongoClient

logging.basicConfig()
logger = logging.getLogger("add_customers")
logger.setLevel(logging.INFO)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="recalculate pre-plans")
    #parser.add_argument("userid")
    parser.add_argument("roster")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--api-server", default="https://app.flowmsp.com")
    parser.add_argument("--customer-name", default="customer_name")
    parser.add_argument("--email", default="email")
    parser.add_argument("--password", default="password")
    parser.add_argument("--first-name", default="first_name")
    parser.add_argument("--last-name", default="last_name")
    parser.add_argument("--address", default="address")
    parser.add_argument("--city", default="city")
    parser.add_argument("--state", default="state")
    parser.add_argument("--zip", default="zip")

    args = parser.parse_args()

    url = f"{args.api_server}/api/signup"

    fp = open(args.roster)
    r = csv.DictReader(fp)

    # convert all column names as all lower case
    r.fieldnames = [h.lower() for h in r.fieldnames]
    
    for rec in r:
        customer = {
            "customerName": rec[args.customer_name],
            "email": rec[args.email],
            "password": rec[args.password],
            "firstName": rec.get(args.first_name, ""),
            "lastName": rec.get(args.last_name, ""),
            "address": {
                "address1": rec.get(args.address, ""),
                "city": rec.get(args.city, ""),
                "state": rec.get(args.state, ""),
                "zip": rec.get(args.zip, ""),
            }
        }

        logger.debug(customer)

        r = requests.post(url, json=customer)

        if not r.ok:
            logger.warning("create failed: %s" % rec)
            logger.warning(r.text)
        else:
            jdata = r.json()

            print(jdata["customerId"],
                  jdata["customerName"],
                  jdata["customerSlug"],
                  jdata["email"],
                  jdata["firstName"],
                  jdata["lastName"])


    

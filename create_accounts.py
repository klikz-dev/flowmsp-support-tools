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


# need to get lat/lon
# generate slug
# what to do about license
# geocode data
def create_customer(db, rec):
    pprint.pprint(rec)
    created = datetime.datetime.now()
    expires = created + datetime.timedelta(days=30)

    orec = {
        "_id": "ea7a5f98-a1cc-4ad6-bed1-e93d1b2b4bf2",
        "slug": "jacksonville",
        "address": {"address1": rec["HQ addr1"],
                    "city": rec["HQ city"],
                    "latLon": {"coordinates": [-90.2306956, 39.7364451],
                               "type": "Point"},
                    "state": rec["HQ state"],
                    "zip": rec["HQ zip"],
                    },
        "boundNELat": 0.0,
        "boundNELon": 0.0,
        "boundSWLat": 0.0,
        "boundSWLon": 0.0,
        "dataSharingConsent": False,
        "dispatchSharingConsent": False,
        "license": {"creationTimestamp": created,
                    "expirationTimestamp": expires,
                    "licenseTerm": "Monthly",
                    "licenseType": "Preview"},
        "name": rec["Fire dept name"],
        "pinLegend": {"rangePinColors": [{"high": 500,
                                          "label": "0 to less than 500 GPM",
                                          "low": 0,
                                          "pinColor": "RED"},
                                         {"high": 1000,
                                          "label": "500 to less than 1000 GPM",
                                          "low": 500,
                                          "pinColor": "ORANGE"},
                                         {"high": 1500,
                                          "label": "1000 to less than 1500 GPM",
                                          "low": 1000,
                                          "pinColor": "GREEN"},
                                         {"high": 100000,
                                          "label": "1500+ GPM",
                                          "low": 1500,
                                          "pinColor": "BLUE"}],
                      "unknownPinColor": {"high": 0,
                                          "label": "Unknown",
                                          "low": 0,
                                          "pinColor": "YELLOW"}},
        "settings": {"minimumNewHydrantDistance": 100,
                     "preplanningAreaRounding": 100,
                     "preplanningMaxAreaForFlowComputation": 20000,
                     "preplanningMaxDistanceForHydrantSearch": 5000,
                     "preplanningMaxHydrants": 10}
    }
                                                                     
    pprint.pprint(orec)

    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="check for broken links")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("--limit", type=int)

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")
    image_uri = get_parm(session, "image_uri")

    client = MongoClient(mongo_uri)

    db = client.FlowMSP

    for recnum, rec in enumerate(csv.DictReader(sys.stdin)):
        if args.limit and recnum >= args.limit:
            break

        create_customer(db, rec)


#!/usr/bin/env python3

import os
import sys
import argparse
from pymongo import MongoClient
import boto3
import botocore
import pickle
import logging


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="dump hydrants")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("slug")
    parser.add_argument("dump_file")

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")
    client = MongoClient(mongo_uri)
    db = client.FlowMSP

    logging.basicConfig(level=logging.INFO)

    collection = "%s.Hydrant" % args.slug
    rows = [row for row in db[collection].find()]

    fp = open(args.dump_file, "wb")
    pickle.dump(rows, fp)
    fp.close()

    logging.info("dumped %d hydrants" % len(rows))

    

#!/usr/bin/env python3

import os
import sys
import uuid
import argparse
import boto3
import pprint
import pymongo
from pymongo import MongoClient
import geojson
import logging
import traceback
import pdb


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


def get_db(profile_name, db_url=None):
    session = boto3.session.Session(profile_name=profile_name)
    mongo_uri = get_parm(session, "mongo_uri")

    if db_url:
        client = MongoClient(db_url)
    else:
        client = MongoClient(mongo_uri)

    db = client.FlowMSP

    return db, client


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="add index on message_id to all dispatch tables that don't have one")
    parser.add_argument("profile", choices=["flowmsp-dev", "flowmsp-prod"])

    args = parser.parse_args()

    db, client = get_db(args.profile)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

    filter = {"name": {"$regex": r"\.MsgReceiver"}}

    for cnum, c in enumerate(db.list_collection_names(filter=filter)):
        if cnum >= 10:
            break

        indexes = db[c].index_information()
        for index_name in indexes:
            print(c, index_name)

        if "messageID" in db[c].index_information():
            logging.info("%s: index already exists" % c)
            continue
        
        #db[c].create_index("messageID")
        logging.info("%s: index created" % c)

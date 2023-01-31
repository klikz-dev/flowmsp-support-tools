#!/usr/bin/env python3

import os
import sys
import pprint
import argparse
from pymongo import MongoClient
import boto3
import botocore

def connect(url):
    client = MongoClient(url)

    return client

def copy_collection(srcdb, dstdb, collection, truncate, limit):
    if collection not in dstdb.collection_names():
        dstdb.create_collection(collection)
        print("%s: created" % collection)

    if truncate:
        result = dstdb[collection].delete_many({})
        print("%s: deleted %d rows" % (collection, result.deleted_count))

    rows = 0
    for row in fromDB[c].find():
        rows += 1
        toDB[c].insert_one(row)

    print("%s: inserted %d rows" % (collection, rows))

    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="copy database from src to dst")
    parser.add_argument("database")
    parser.add_argument("src")
    parser.add_argument("dst")
    parser.add_argument("--truncate", action="store_true")
    parser.add_argument("--limit", default="all")

    args = parser.parse_args()

    fromClient = connect(args.src)
    fromDB = fromClient[args.database]

    toClient = connect(args.dst)
    toDB = toClient[args.database]

    for c in fromDB.list_collections():
        copy_collection(fromDB, toDB, c, args.truncate, args.limit)

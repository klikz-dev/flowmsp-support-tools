#!/usr/bin/env python3

import os
import sys
import pprint
import argparse
from pymongo import MongoClient
import boto3
import botocore

def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


class EnvInfo:
    def __init__(self, profile_name):
        self.profile_name = profile_name
        self.session = boto3.session.Session(profile_name=profile_name)
        self.s3url = get_parm(self.session, "image_uri")
        self.mongo_url = get_parm(self.session, "mongo_uri")
        self.client = MongoClient(self.mongo_url)
        self.db = self.client.FlowMSP


def copy_table(src, dst, table):
    if table in dst.db.list_collection_names():
        dst.db.drop_collection(table)
        print("%s: dropped" % table)

    dst.db.create_collection(table)
    print("%s: created" % table)

    # create indexes
    for name, index_info in src.db[table].index_information().items():
        keys = index_info['key']
        del(index_info['ns'])
        del(index_info['v'])
        del(index_info['key'])
        dst.db[table].create_index(keys, name=name, **index_info)
        print("%s: %s: index created" % (table, name))

    rows = 0
    batch = []
    fix_urls = False

    est_rows = src.db[table].count_documents({})

    print("src table contains %d rows" % est_rows)

    for src_row in src.db[table].find():
        rows += 1

        batch.append(src_row)

        if len(batch) >= 500:
            dst.db[table].insert_many(batch)
            print("inserted batch, rows=%d" % rows)
            batch = []

    if batch:
        dst.db[table].insert_many(batch)

    print("%s: inserted % d rows" % (table, rows))
    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="copy collections from src to dst")
    parser.add_argument("src")
    parser.add_argument("dst")
    parser.add_argument("collections", nargs="+")
    parser.add_argument("--verbose", action="store_true")

    args = parser.parse_args()

    src = EnvInfo(args.src)
    dst = EnvInfo(args.dst)

    for c in args.collections:
        copy_table(src, dst, c)

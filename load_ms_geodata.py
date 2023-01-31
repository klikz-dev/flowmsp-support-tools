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


def get_db(profile_name, db_url):
    session = boto3.session.Session(profile_name=profile_name)
    mongo_uri = get_parm(session, "mongo_uri")

    if db_url:
        client = MongoClient(db_url)
    else:
        client = MongoClient(mongo_uri)

    db = client.FlowMSP

    return db, client

def get_batches(features, batch_size):
    batch = []

    for f in features:
        if len(batch) == batch_size:
            yield batch
            batch = []

        if "_id" not in f:
            f["_id"] = str(uuid.uuid4())

        batch.append(f)

    yield batch


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="load MS building polygons into mongodb")
    parser.add_argument("profile", choices=["flowmsp-dev", "flowmsp-prod"])
    parser.add_argument("geojson_file")
    parser.add_argument("collection")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--truncate", action="store_true")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--count", type=int, default=5000)
    parser.add_argument("--create-index", action="store_true")
    parser.add_argument("--error-file", default="errors.geojson")
    parser.add_argument("--db-url")

    args = parser.parse_args()

    db, client = get_db(args.profile, args.db_url)

    if args.debug:
        logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(message)s")
    else:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

    if args.collection in db.list_collection_names() and args.truncate:
        db.drop_collection(args.collection)
        logging.info("%s dropped" % args.collection)

    if args.collection not in db.list_collection_names():
        db.create_collection(args.collection)

        print("%s: created collection" % args.collection,
              file=sys.stderr)

        if args.create_index:
            db[args.collection].create_index([("geometry", pymongo.GEOSPHERE)])

    logging.info("loading features")
    data = geojson.load(open(args.geojson_file))
    print("loaded %d features" % len(data["features"]), file=sys.stderr)

    batch = []
    batch_errors = []
    count = 0

    for bnum, batch in enumerate(get_batches(data["features"], args.batch_size)):
        if args.limit and bnum >= args.limit:
            break

        logging.debug("bnum=%d" % bnum)

        try:
            r = db[args.collection].insert_many(batch)

            if len(r.inserted_ids) != len(batch):
                logging.warning("bnum=%d insert_many failed, len(r.inserted_id)=%d" %
                                (bnum, len(r.inserted_ids)))
                raise

        except Exception as e:
            logging.error("bnum=%d: insert_many failed" % bnum)
            logging.error(traceback.format_exc())
            logging.error(e.details)
            batch_errors += batch
            
        if args.verbose:
            logging.info("wrote batch %d" % bnum)

    # end for

    print("dumping %d errors to %s" %
          (len(batch_errors), args.error_file), file=sys.stderr)
    
    fpERR = open(args.error_file, "w")
    collection = geojson.FeatureCollection(batch_errors)
    geojson.dump(collection, fpERR)

    
        
        

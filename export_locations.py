#!/usr/bin/env python3

import os
import sys
import argparse
import logging
import boto3
import botocore
from utils import get_db
import geojson


logging.basicConfig()
logger = logging.getLogger("export_locations")


def process_slugs(db, slugs):
    for slug in slugs:
        collection = "%s.Location" % slug
        rows = 0

        for row in db[collection].find():
            rows += 1
            yield row

        logger.info("%s: rows=%d" % (slug, rows))

    return










if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="export locations in geoJSON format")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("slugs", nargs="+")
    parser.add_argument("--db-url")

    args = parser.parse_args()
    logger.setLevel(logging.INFO)

    session = boto3.session.Session(profile_name=args.profile)
    client, db = get_db(session, args.db_url)

    features = []

    for row in process_slugs(db, args.slugs):
        f = {
            "type": "Feature",
            "_id": row["_id"],
            "geometry": row["geoOutline"],
            "properties": {
                "batch_no": row.get("batchNo", ""),
                "name": row.get("name", ""),
                "customer_slug": row.get("customerSlug", ""),
                "notes": row.get("notes", "").replace("\n", ""),
                "address": row.get("address", {}).get("address1", ""),
            }
        }

        features.append(f)

    collection = geojson.FeatureCollection(features)

    geojson.dump(collection, sys.stdout)

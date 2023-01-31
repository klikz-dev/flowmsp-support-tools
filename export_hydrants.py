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
logger = logging.getLogger("export_hydrants")


def process_slugs(db, slugs, limit):
    for slugnum, slug in enumerate(slugs):
        if limit and slugnum >= limit:
            break

        collection = "%s.Hydrant" % slug
        rows = 0

        for row in db[collection].find():
            rows += 1
            yield row

        logger.info("%s: rows=%d" % (slug, rows))

    return


def get_slugs(db):
    return [c["slug"] for c in db.Customer.find()]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="export hydrants")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("--limit", type=int)
    parser.add_argument("--db-url")
    parser.add_argument("--slugs", nargs="+")

    args = parser.parse_args()
    logger.setLevel(logging.INFO)

    session = boto3.session.Session(profile_name=args.profile)
    client, db = get_db(session, args.db_url)

    features = []

    slugs = args.slugs or get_slugs(db)

    for rownum, row in enumerate(process_slugs(db, slugs, args.limit)):
        f = {
            "type": "Feature",
            "_id": row["_id"],
            "geometry": row["lonLat"],
            "properties": {
                "slug": row.get("customerSlug", ""),
            }
        }

        features.append(f)

    collection = geojson.FeatureCollection(features)

    geojson.dump(collection, sys.stdout)

#!/usr/bin/env python3

import os
import sys
import pprint
import argparse
from pymongo import MongoClient
import boto3
import botocore
import logging
from geo_location import GeoLocation


logging.basicConfig()
logger = logging.getLogger("add_bounding_box")


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


def get_db(profile):
    session = boto3.session.Session(profile_name=profile)
    mongo_uri = get_parm(session, "mongo_uri")

    client = MongoClient(mongo_uri)
    db = client.FlowMSP

    return db, client


def create_bounding_box(midLat, midLon, miles):
    logger.debug("in createBoundingBox")
    feet_in_mile = 5280
    length = miles * feet_in_mile
    distanceinFeet = (1.0 / 1.41421356237) * length

    logger.debug("distanceinFeet=%s" % distanceinFeet)

    earthRadiusinFeet = 20902263.78
    
    logger.debug("midLat=%s" % midLat)
    logger.debug("midLon=%s" % midLon)
    
    midLoc = GeoLocation().fromDegrees(midLat, midLon)
    logger.debug("midLoc=%s" % midLoc)

    boundedBox = midLoc.boundingCoordinates(distanceinFeet, earthRadiusinFeet)
    logger.debug("boundedBox[0]=%s" % boundedBox[0])
    logger.debug("boundedBox[1]=%s" % boundedBox[1])

    return {
        "boundSWLat": boundedBox[0].getLatitudeInDegrees(),
        "boundSWLon": boundedBox[0].getLongitudeInDegrees(),
        "boundNELat": boundedBox[1].getLatitudeInDegrees(),
        "boundNELon": boundedBox[1].getLongitudeInDegrees()
    }
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="copy hydrants")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("--slug", nargs="+")
    parser.add_argument("--update", action="store_true")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--miles", type=int, default=100)

    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    db, client = get_db(args.profile)

    query = {}

    if args.slug:
        query["slug"] = args.slug[0]

    if args.slug is None and args.force is False:
        for k in "boundSWLat", "boundSWLon", "boundNELat", "boundNELon":
            query[k] = 0.0

    rows = updated = 0
    logger.info(query)

    for rownum, row in enumerate(db["Customer"].find(query)):
        if args.limit and rownum >= args.limit:
            break

        rows += 1
        logger.debug(row)

        s = ["%s=%s" % (k, row[k]) for k in ["boundSWLat", "boundSWLon", "boundNELat", "boundNELon"]]
        logger.info("%s: BEFORE: %s" % (row.get("slug", ""), s))

        lonlat = row.get("address", {}).get("latLon", {}).get("coordinates", [])

        if lonlat:
            lat, lon = lonlat[1], lonlat[0]

            r = create_bounding_box(lat, lon, args.miles)
            logger.debug(r)

            for k in r:
                row[k] = r[k]

        else:
            logger.info("%s: no lat/lon" % row.get("slug", ""))
            continue

        logger.debug(row)
        s = ["%s=%s" % (k, row[k]) for k in ["boundSWLat", "boundSWLon", "boundNELat", "boundNELon"]]
        logger.info("%s: AFTER: %s" % (row.get("slug", ""), s))

        if args.update:
            r = db["Customer"].replace_one({"_id": row["_id"]}, row)

            if r:
                updated += 1


    logger.info("rows=%d" % rows)
    logger.info("updated=%d" % updated)
    

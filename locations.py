#!/usr/bin/env python3

import os
import sys
import pprint
import argparse
import json
import csv
from pymongo import MongoClient
import boto3
import botocore
import logging
from utils import get_db


logging.basicConfig()
logger = logging.getLogger("locations")
logger.setLevel(logging.INFO)


default_hdr = [
    "Polygon",
    "Name",
    "Street Address",
    "Street Address2",
    "City",
    "State",
    "Zip code",
    "Roof Area (Sq. Ft)",
    "Occupancy Type",
    "Construction Type",
    "Roof Type",
    "Roof Construction",
    "Roof Material",
    "Normal Population",
    "Sprinklered",
    "Stand Pipe",
    "Fire Alarm",
    "Hours of Operation",
    "Owner Contact",
    "Owner Phone",
    "Notes",
    "Storey Above",
    "Storey Below"
]


def get_polygon_string(row):
    if "coordinates" not in row.get("geoOutline", {}):
        return ""

    points = []

    for lon, lat in row["geoOutline"]["coordinates"][0]:
        p = ":".join([str(lat), str(lon)])
        points.append(p)

    return "|".join(points)

    
def make_record(row, hdr):
    orec = {
        "Polygon":            get_polygon_string(row),
        "Name":               row.get("name", ""),
        "Street Address":     row.get("address", {}).get("address1", ""),
        "Street Address2":    row.get("address", {}).get("address2", ""),
        "City":               row.get("address", {}).get("city", ""),
        "State":              row.get("address", {}).get("state", ""),
        "Zip code":           row.get("address", {}).get("zip", ""),
        "Roof Area (Sq. Ft)": row.get("roofArea"),
        "Occupancy Type":     row.get("building", {}).get("occupancyType"),
        "Construction Type":  row.get("building", {}).get("constructionType"),
        "Roof Type":          row.get("building", {}).get("roofType"),
        "Roof Construction":  row.get("building", {}).get("roofConstruction"),
        "Roof Material":      row.get("building", {}).get("roofMaterial"),
        "Normal Population":  row.get("building", {}).get("normalPopulation"),
        "Sprinklered":        row.get("building", {}).get("sprinklered"),
        "Stand Pipe":         row.get("building", {}).get("standPipe"),
        "Fire Alarm":         row.get("building", {}).get("fireAlarm"),
        "Hours of Operation": row.get("building", {}).get("hoursOfOperation"),
        "Owner Contact":      row.get("building", {}).get("ownerContact"),
        "Owner Phone":        row.get("building", {}).get("ownerPhone"),
        "Notes":              row.get("notes"),
        "Storey Above":       row.get("storey"),
        "Storey Below":       row.get("storeyBelow"),
    }

    return orec


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="check for broken links")
    parser.add_argument("slug")
    parser.add_argument("--profile", default="flowmsp-prod")
    parser.add_argument("--truncate", action="store_true")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--db-url")
    parser.add_argument("--extra-fields")
    parser.add_argument("--query", default="{}")

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    client, db = get_db(session, args.db_url)

    coll = "%s.Location" % args.slug

    if args.truncate:
        #result = db[coll].delete_many(query)
        print("deleted %d rows" % result.deleted_count)
        raise SystemExit(0)

    query = {}

    hdr = default_hdr

    w = csv.DictWriter(sys.stdout, hdr, lineterminator="\n")
    w.writeheader()

    inrecs = 0

    for rownum, row in enumerate(db[coll].find(query)):
        if args.limit and rownum >= args.limit:
            break

        inrecs += 1

        orec = make_record(row, hdr)
        w.writerow(orec)

    logger.info(f"inrecs={inrecs}")


#!/usr/bin/env python3

import os
import sys
import uuid
import pprint
import time
import json
import argparse
import csv
import random
import datetime
from pymongo import MongoClient
import boto3
import botocore
import shapely
import shapely.geometry
import requests
import geojson


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


def get_customer_id(db, slug):
    row = db.Customer.find_one({"slug": slug})

    return row["_id"]


def load_geojson(filename, element):
    data = json.load(open(filename))
    index = 0

    if element:
        index = int(element)

    # geoOutline from a location for example
    if type(data) is dict and "geometry" in data:
        return shapely.geometry.asShape(data["geometry"])

    elif type(data) is dict and "coordinates" in data:
        return shapely.geometry.asShape(data)

    elif type(data) is dict and "geoOutline" in data:
        print(data["geoOutline"]["type"], file=sys.stderr)
        return shapely.geometry.asShape(data["geoOutline"])

    elif type(data) is dict and data["type"] == "FeatureCollection":
        f = data["features"][index]
        print(f["properties"], file=sys.stderr)
        return shapely.geometry.asShape(f["geometry"])

    elif type(data) is list and "display_name" in data[0]:
        print("display_name", data[0]["display_name"], file=sys.stderr)
        return shapely.geometry.asShape(data[0]["geojson"])

    else:
        print("unknown type of data", file=sys.stderr)
        raise SystemExit(1)
    

base_url = "https://maps.googleapis.com/maps/api/geocode/json"

def get_address(result):
    street_number = route = postal_code = locality = administrative_area_level_1 = ""
    
    for r in result["address_components"]:
        if "street_number" in r["types"]:
            street_number = r["long_name"]

        elif "route" in r["types"]:
            route = r["short_name"]

        elif "postal_code" in r["types"]:
            postal_code = r["long_name"]

        elif "locality" in r["types"]:
            locality = r["long_name"]

        elif "administrative_area_level_1" in r["types"]:
            administrative_area_level_1 = r["short_name"]

    return {
        "address1": " ".join([street_number, route]),
        "address2": "",
        "city": locality,
        "state": administrative_area_level_1,
        "zip": postal_code
    }

            

def reverse_geocode(polygon, map_key):
    polygon = shapely.geometry.asShape(polygon)
    p = polygon.centroid
    url = "{url}?latlng={lat},{lon}&key={key}".format(
        url=base_url,
        lat=p.y,
        lon=p.x,
        key=map_key)

    r = requests.get(url)

    if not r.ok:
        return

    jdata = r.json()

    # this will need some work
    # for now, return the first street address
    for r in jdata["results"]:
        #pprint.pprint(r)

        if "street_address" in r["types"]:
            return get_address(r)

    return


def force_coordinates_to_floats(feature):
    coords = feature["geometry"]["coordinates"][0]

    for i, c in enumerate(coords):
        for j, v in enumerate(c):
            if type(v) is int:
                coords[i][j] = float(coords[i][j])

    return


def load_locations(db, collection):
    locations = []

    for row in db[collection].find({}):
        row["shape"] = shapely.geometry.asShape(row["geoOutline"])
        locations.append(row)

    print("loaded %d existing locations" % len(locations),
          file=sys.stderr)

    return locations


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="check for broken links")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("customer_name", help="customer name, aka slug")
    parser.add_argument("geojson_file")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--multi-line", action="store_true")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--truncate", action="store_true")
    parser.add_argument("--batch-size", type=int, default=250)
    parser.add_argument("--skip", type=int)
    parser.add_argument("--only-house-numbers", action="store_true")
    parser.add_argument("--tags", nargs="+")
    parser.add_argument("--geofence", help="geoJSON file")
    parser.add_argument("--geofence-element")
    parser.add_argument("--shuffle", action="store_true", help="randomly shuffle locations before loading")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--ignore-duplicates", action="store_true")
    parser.add_argument("--business-name", default="TBD")
    
    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")
    map_key = get_parm(session, "GOOGLE_MAP_API_KEY")

    PGM = os.path.basename(sys.argv[0])

    client = MongoClient(mongo_uri)
    db = client.FlowMSP

    data = geojson.load(open(args.geojson_file))
    print("features=%d" % len(data["features"]), file=sys.stderr)

    if args.shuffle:
        g = random.Random()
        g.shuffle(data["features"])

    if "time_ns" in dir(time):
        batchNo = time.time_ns()
    else:
        batchNo = int(time.time())

    collection = "%s.Location" % args.customer_name
    customer_id = get_customer_id(db, args.customer_name)
    print("customer_id=%s" % customer_id)

    rows = skipped = 0

    if args.truncate:
        result = db[collection].delete_many({})
        print("deleted %d rows" % result.deleted_count)

    if args.geofence:
        gf = load_geojson(args.geofence, args.geofence_element)
    else:
        gf = None

    batch = []
    existing_locations = load_locations(db, collection)


    for rownum, feature in enumerate(data["features"]):
        if args.skip and rownum <= args.skip:
            continue

        if args.limit and rows >= args.limit:
            break

        if args.debug:
            print("rownum", rownum, file=sys.stderr)
            print(feature["properties"], file=sys.stderr)

        if args.verbose:
            if rownum % 25 == 0:
                print("rownum=%d rows=%d skipped=%d" % (rownum, rows, skipped), file=sys.stderr)

        now = datetime.datetime.now()

        props = feature["properties"]
        street = props.get("street", "")
        hnum = props.get("estimatedHouseNumber", "")
        addr1 = " ".join([hnum, street])

        if args.only_house_numbers and "estimatedHouseNumber" not in props:
            skipped += 1
            continue

        if args.tags:
            if "@ns:com:here:xyz" not in props:
                skipped += 1
                continue

            if "tags" not in props["@ns:com:here:xyz"]:
                skipped += 1
                continue

            tags = props["@ns:com:here:xyz"]["tags"]

            not_found = 0
            for t in args.tags:
                if t not in tags:
                    not_found += 1

            if not_found > 0:
                # print("skipping", tags)
                skipped += 1
                continue

        if gf:
            p = shapely.geometry.asShape(feature["geometry"])

            if gf.contains(p) is False:
                skipped += 1
                continue

        rows += 1

        # make sure coordinates are floats
        force_coordinates_to_floats(feature)

        row = {
            "_id": str(uuid.uuid4()),
            "address": {
                "address1": addr1,
                "address2": "",
                "city": props.get("city", ""),
                "state": props.get("state", ""),
                "zip": props.get("postalcode", ""),
            },
            "batchNo": batchNo,
            "createdOn": now,
            "createdBy": PGM,
            "customerSlug": args.customer_name,
            "customerId": customer_id,
            "name": args.business_name,
            "images": [],
            "hydrants": [],
            "building": {
                "normalPopulation": "",
                "lastReviewedBy": PGM,
                "lastReviewedOn": now.strftime("%m-%d-%Y %H.%M.%S"),
                "originalPrePlan": now.strftime("%m-%d-%Y %H.%M.%S"),
            },
            "roofArea": 0,
            "geoOutline": feature["geometry"],
        }

        # "requiredFlow": 0,

        if "standardized_address" in props:
            rdi = props["standardized_address"]["metadata"].get("rdi", "n/a")

            # Industrial
            if rdi == "Commercial":
                row["building"]["occupancyType"] = "Business / Mercantile"

        # Vacant

        if args.debug:
            print("------------------------------------------------------------")
            print("rownum=%d" % rownum)
            print("feature")
            pprint.pprint(feature)
            print()
            print("row")
            pprint.pprint(row)

        s = shapely.geometry.asShape(feature["geometry"])
        dups = 0

        if args.ignore_duplicates is False:
            for e in existing_locations:
                if s.intersects(e["shape"]):
                    print("polygon overlaps", e["address"], file=sys.stderr)
                    dups += 1

        if dups:
            print("duplicate", row["address"], file=sys.stderr)
            continue

        batch.append(row)

        if len(batch) == args.batch_size:
            if args.dry_run is False:
                db[collection].insert_many(batch)
            batch = []

    if batch:
        if args.dry_run is False:
            db[collection].insert_many(batch)

    print("rows=%d skipped=%d" % (rows, skipped), file=sys.stderr)

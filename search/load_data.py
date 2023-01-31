#!/usr/bin/env python3

import os
import sys
import json
import time
import argparse
import pprint
import urllib.parse
import requests
import shapely
import shapely.geometry
from pymongo import MongoClient
import pymongo
from bson import ObjectId
import boto3
import botocore

# https://nominatim.openstreetmap.org/search.php?q=glen+carbon%2C+IL&polygon_geojson=1&format=json
base_url = "https://nominatim.openstreetmap.org/search.php"

def get_geojson(place):
    url = "{base}?q={place}&polygon_geojson=1&format=json".format(
        base=base_url,
        place=urllib.parse.quote(place.replace(" ", "+"), safe="+")
    )
    # url = "https://nominatim.openstreetmap.org/search.php?q=glen+carbon%2C+IL&polygon_geojson=1&format=json"
    print("url=%s" % url, file=sys.stderr)

    r = requests.get(url)

    if r.ok:
        return r.json()


def get_geofence(geometry):
    print(geometry["display_name"], file=sys.stderr)

    return shapely.geometry.asShape(geometry["geojson"])


def within_polygon(loc, polygon):
    # polygon = shapely.geometry.asShape(feature["geometry"])

    return polygon.contains(loc["Point"])


def now():
    return time.strftime("%Y-%m-%d %H:%M:%S")

def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


def get_db(profile, dbname):
    session = boto3.session.Session(profile_name=profile)
    mongo_uri = get_parm(session, "mongo_uri")

    client = MongoClient(mongo_uri)
    db = client[dbname]
    return db


def geocode(street_address, map_key):
    place=urllib.parse.quote(street_address.replace(" ", "+"), safe="+")
    base_url = "https://maps.googleapis.com/maps/api/geocode/json"
    url = "{url}?address={addr}&key={key}".format(
        url=base_url,
        addr=place,
        key=map_key)

    r = requests.get(url)

    if not r.ok:
        return
    
    latlon = r.json()["results"][0]["geometry"]["location"]

    # turn into a geojson point
    return {
        "geojson": {
            "location": {
                "type": "Point",
                "coordinates": [latlon["lng"], latlon["lat"]]
            },
            "name": street_address
        }
    }
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="fix test URLs in annotations")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("database")
    parser.add_argument("collection")
    parser.add_argument("index")

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")
    map_key = get_parm(session, "GOOGLE_MAP_API_KEY")

    client = MongoClient(mongo_uri)
    db = client[args.database]

    if args.collection not in db.list_collection_names():
        db.create_collection(args.collection)

    for line in sys.stdin:
        row = json.loads(line)

        # remove oid
        row.pop("_id")
        
        db[args.collection].insert_one(row)

    db[args.collection].create_index([(args.index, pymongo.GEOSPHERE)])

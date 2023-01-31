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


def get_db(profile):
    session = boto3.session.Session(profile_name=profile)
    mongo_uri = get_parm(session, "mongo_uri")

    client = MongoClient(mongo_uri)
    db = client.FlowMSP
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
    parser.add_argument("place")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--progress", action="store_true")
    parser.add_argument("--count", type=int, default=100000)

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")
    map_key = get_parm(session, "GOOGLE_MAP_API_KEY")

    client = MongoClient(mongo_uri)
    db = client.FlowMSP

    # this didn't seem to work
    #r = get_geojson(args.place)
    #gf = get_geofence(r[0])

    # let's try a lat/lon
    latlon = geocode(args.place, map_key)

    # query = {"geometry": {"$geoWithin": { "$geometry": r[0]["geojson"]}}}
    # query = {"geometry": {"$geoIntersects": { "$geometry": r[0]["geojson"]}}}
    query = {
        "geoOutline": {
            "$geoIntersects": {
                "$geometry": {
                    "type": "Point",
                    "coordinates": [
                        latlon["geojson"]["location"]["coordinates"][0],
                        latlon["geojson"]["location"]["coordinates"][1]
                    ]
                }
            }
        }
    }

    if args.debug:
        pprint.pprint(query)

    rows = 0

    for row in db.ms_geojson.find(query):
        rows += 1
        pprint.pprint(row)

    for row in db.ms_geojson.find():
        pass

    print("rows=%d" % rows)


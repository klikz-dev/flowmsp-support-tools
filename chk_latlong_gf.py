#!/usr/bin/env python3

import os
import sys
import argparse
import requests
import boto3
import json
import pprint
from pymongo import MongoClient
import shapely
import shapely.geometry

def get_customers(db):
    return [row["slug"] for row in db.Customer.find()]
        

def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


def within_polygon(point, polygon):
    # polygon = shapely.geometry.asShape(feature["geometry"])

    return polygon.contains(point)


def get_geofence(db, slug):
    row = db.Customer.find_one({"slug": slug})

    if not row:
        raise Exception("%s: customer not found" % slug)

    ne_lat = row.get("boundNELat", 0.0)
    ne_lon = row.get("boundNELon", 0.0)
    sw_lat = row.get("boundSWLat", 0.0)
    sw_lon = row.get("boundSWLon", 0.0)

    geometry = {
        "type": "Polygon",
        "coordinates": [
            [   [ne_lon, ne_lat], # ne
                [ne_lon, sw_lat], # se
                [sw_lon, sw_lat], # sw
                [sw_lon, ne_lat], # nw
                [ne_lon, ne_lat], # ne close polygon
            ]],
        "properties": {}
    }

    pprint.pprint(geometry)

    return shapely.geometry.asShape(geometry)


def get_latlon(row, map_key):
    addr = row["address"]

    address = ",".join([
        addr["address1"],
        addr["city"],
        addr["state"],
        addr["zip"]])

    base_url = "https://maps.googleapis.com/maps/api/geocode/json"
    url = "{url}?address={addr}&key={key}".format(
        url=base_url,
        addr=address,
        key=map_key)

    r = requests.get(url)
    results = r.json()
    latlon = results["results"][0]["geometry"]["location"]

    row["lat"] = latlon["lat"]
    row["lng"] = latlon["lng"]
    row["Point"] = shapely.geometry.Point(row["lng"], row["lat"])

    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="fix test URLs in annotations")
    parser.add_argument("profile", choices=["flowmsp-dev", "flowmsp-prod"])
    parser.add_argument("slug")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--progress", action="store_true")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--limit", type=int)

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")
    map_key = get_parm(session, "GOOGLE_MAP_API_KEY")

    client = MongoClient(mongo_uri)

    db = client.FlowMSP

    gf = get_geofence(db, args.slug)

    for line in sys.stdin:
        flds = line.strip().split()
        lat = float(flds[0])
        lon = float(flds[1])

        point = shapely.geometry.Point(lon, lat)

        print(lat, lon, within_polygon(point, gf))


        

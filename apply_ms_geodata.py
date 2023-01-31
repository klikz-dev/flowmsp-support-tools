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


def within_polygon(loc, polygon):
    # polygon = shapely.geometry.asShape(feature["geometry"])

    return polygon.contains(loc["Point"])


def get_geofence(locations):
    max_lat = max([loc["lat"] for loc in locations])
    min_lat = min([loc["lat"] for loc in locations])
    max_lng = max([loc["lng"] for loc in locations])
    min_lng = min([loc["lng"] for loc in locations])

    geometry = {
        "type": "Polygon",
        "coordinates": [
            [   [max_lng, max_lat], # ne
                [max_lng, min_lat], # se
                [min_lng, min_lat], # sw
                [min_lng, max_lat], # nw
                [max_lng, max_lat], # ne close polygon
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


def load_locations(db, slug, map_key, limit, verbose):
    locations = []
    rows = 0

    for row in db["%s.Location" % slug].find():
        rows += 1
        get_latlon(row, map_key)
        locations.append(row)

        if limit and rows == limit:
            break

        if verbose:
            print(rows, row["name"], row["address"]["address1"], row["lat"], row["lng"])

    return locations
    

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

    locations = load_locations(db, args.slug, map_key, args.limit, args.verbose)
    print("loaded %d locations" % len(locations))

    gf = get_geofence(locations)

    lines = features_read = features_loaded = 0
    count = in_geofence = 0

    for line in sys.stdin:
        lines += 1
        count += 1

        if args.progress and count == 100000:
            print("lines=%d in_geofence=%d" % (lines, in_geofence),
                  file=sys.stderr)
            count = 0


        if args.debug:
            print("lines=%d features_loaded=%d batch=%d" %
                  (lines, features_loaded, len(batch)),
                  file=sys.stderr)

        if '"Feature"' in line:
            line = line.strip()
            features_read += 1

            if line.endswith(","):
                line = line[:-1]

            jdata = json.loads(line)

            if args.debug:
                pprint.pprint(jdata)

            polygon = shapely.geometry.asShape(jdata["geometry"])

            if not gf.contains(polygon):
                continue

            in_geofence += 1

            for loc in locations:
                if polygon.contains(loc["Point"]):
                    row = loc.copy()
                    row["geoOutline"] = {"type": "Polygon"}
                    row["geoOutline"]["coordinates"] = jdata["geometry"]["coordinates"]
                    row.pop("Point")

                    db["%s.Location" % args.slug].replace_one({"_id": loc["_id"]}, row)

                    pprint.pprint(jdata)

    print("features_read=%d features_loaded=%d" %
          (features_read, features_loaded),
          file=sys.stderr)


        
        

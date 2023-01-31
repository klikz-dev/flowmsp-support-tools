#!/usr/bin/env python3

import sys
import argparse
import json
import pprint
import csv
import urllib.parse
import requests
import geojson
import shapely
import shapely.geometry
from pymongo import MongoClient
import boto3
import botocore


def get_outline(db, lat, lon):
    query = {
        "geometry": {
            "$geoIntersects": {
                "$geometry": {
                    "type": "Point",
                    "coordinates": [ lon, lat ]
                }
            }
        }
    }

    row = db.ms_geodata.find_one(query)

    return row


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


def within_polygon(loc, point):
    # polygon = shapely.geometry.asShape(feature["geometry"])

    # return polygon.contains(loc["centroid"])
    return loc["polygon"].contains(point)


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
    
    results = r.json()

    # print(results, file=sys.stderr)

    if "results" in results:
        latlon = results["results"][0]["geometry"]["location"]

        coord = {
            "geojson": {
                "location": {
                    "type": "Point",
                    "coordinates": [latlon["lng"], latlon["lat"]]
                },
                "name": street_address
            }
        }
    else:
        coord = {}

    return coord, results
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="check for broken links")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("existing_locations")
    parser.add_argument("rms_data")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--skip", type=int)
    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")
    map_key = get_parm(session, "GOOGLE_MAP_API_KEY")

    client = MongoClient(mongo_uri)
    db = client.FlowMSP

    locations = json.load(open(args.existing_locations))

    for loc in locations:
        polygon = shapely.geometry.asShape(loc["geoOutline"])
        loc["polygon"] = polygon
        loc["centroid"] = polygon.centroid
        loc["matches"] = 0

    inrecs = num_matches = skipped = 0

    features = []

    r = csv.reader(open(args.rms_data))
    hdr = next(r)
    output = []

    for recnum, rec in enumerate(r):
        inrecs += 1

        if args.skip and recnum < args.skip:
            continue

        if args.limit and recnum >= args.limit:
            break

        street_address = " ".join([rec[1], rec[2]]) + ", Saint Louis, MO"

        r, results = geocode(street_address, map_key)
        matches = []

        if "geojson" in r:
            lat = r["geojson"]["location"]["coordinates"][1]
            lon = r["geojson"]["location"]["coordinates"][0]

            point = shapely.geometry.Point(lon, lat)

            for loc in locations:
                if within_polygon(loc, point):
                    loc["matches"] += 1
                    matches.append(loc)
                    # print("match", loc["name"], loc["address"])

        if matches:
            num_matches += 1
            continue
        
        #print(recnum, rec, len(matches))
        print(recnum, rec, file=sys.stderr)

        row = {
            "Project": rec[0],
            "Address": rec[1],
            "Street": rec[2],
            "Suite": rec[3],
            "Eng_Co": rec[4],
            "Shift": rec[5],
            "location": r,
            "geocode_results": results,
        }

        building_outline = get_outline(db, lat, lon)

        if building_outline:
            row["geoOutline"] = building_outline
            
        output.append(row)

    json.dump(output, sys.stdout, indent=4)

    print("inrecs=%d skipped=%d num_matches=%d" % (inrecs, skipped, num_matches), file=sys.stderr)
    raise SystemExit(0)

    # not found
    for loc in locations:
        if loc["matches"] == 0:
            print("nomatch", loc.get("address"), file=sys.stderr)
        

                          

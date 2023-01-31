#!/usr/bin/env python3

import os
import sys
import csv
import re
import time
import argparse
import requests
import pprint
import boto3
import xml.etree.ElementTree as ET
from collections import namedtuple
from pymongo import MongoClient
import shapely
import shapely.geometry
import pdb


def get_outline(collection, lat, lon):
    within = {
        "geometry": {
            "$geoIntersects": {
                "$geometry": {
                    "type": "Point",
                    "coordinates": [ lon, lat ]
                }
            }
        }
    }

    nearby = {
        "geometry": {
            "$near": {
                "$geometry": {
                    "type": "Point",
                    "coordinates": [ lon, lat ]
                },
                "$maxDistance": 10
            }
        }
    }

    #pdb.set_trace()
    return collection.find_one(within) or collection.find_one(nearby)


def get_polygon_string(outline):
    points = []

    for lon, lat in outline["coordinates"][0]:
        p = ":".join([str(lat), str(lon)])
        points.append(p)

    return "|".join(points)


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="attach building footprint")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("collection")
    parser.add_argument("--infile")
    parser.add_argument("outfile_polygons")
    parser.add_argument("outfile_nopolygons")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--latitude", default="lat")
    parser.add_argument("--longitude", default="lon")
    parser.add_argument("--keep", action="store_true")
    parser.add_argument("--polygon-label", action="store_true")
    parser.add_argument("--db-url")

    args = parser.parse_args()
    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")

    if args.db_url:
        client = MongoClient(args.db_url)
    else:
        client = MongoClient(mongo_uri)
        
    db = client.FlowMSP

    if args.infile:
        r = csv.DictReader(open(args.infile))
    else:
        r = csv.DictReader(sys.stdin)
        
    if args.keep:
        hdr = [h for h in r.fieldnames]
    else:
        hdr = [h for h in r.fieldnames if h not in ["addr", "lat", "lon"]]

    wWITH = csv.DictWriter(open(args.outfile_polygons, "w"), hdr, lineterminator="\n")
    wWITH.writeheader()

    wWITHOUT = csv.DictWriter(open(args.outfile_nopolygons, "w"), hdr, lineterminator="\n")
    wWITHOUT.writeheader()

    inrecs = updated = 0
    polygons = set()
    collection = db[args.collection]

    for recnum,rec in enumerate(r):
        if args.limit and recnum >= args.limit:
            break

        inrecs += 1

        try:
            lat = float(rec[args.latitude])
            lon = float(rec[args.longitude])
            outline = get_outline(collection, lat, lon)

            if outline and "geometry" in outline:
                s = get_polygon_string(outline["geometry"])

                if len(s) > 4096:
                    print(recnum, "polygon too long %d" % len(s), rec["Street Address"], file=sys.stderr)

                    if args.polygon_label:
                        rec["Polygon"] = "polygon too long"

                elif s in polygons:
                    if args.polygon_label:
                        rec["Polygon"] = "duplicate"

                    print(recnum, "duplicate polygon", rec["Street Address"], file=sys.stderr)

                else:
                    rec["Polygon"] = s

                    if args.polygon_label:
                        rec["Polygon"] = "polygon"

                    polygons.add(s)
                    updated += 1
            else:
                if args.polygon_label:
                    rec["Polygon"] = "no polygon found"

                print(recnum, "no polygon found", rec["Street Address"], lat, lon, file=sys.stderr)
                    
        except Exception as e:
            print("received exception", e, file=sys.stderr)
            #print(rec, file=sys.stderr)
            pass

        if args.keep is False:
            for k in "addr", "lat", "lon":
                if k in rec:
                    rec.pop(k)

        # trim blanks
        for k in rec:
            rec[k] = rec[k].strip()

        if rec["Polygon"]:
            wWITH.writerow(rec)
        else:
            wWITHOUT.writerow(rec)

    print("inrecs=%d updated=%d" % (inrecs, updated), file=sys.stderr)

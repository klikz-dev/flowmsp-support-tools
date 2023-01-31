#!/usr/bin/env python3

import os
import sys
import string
import csv
import re
import pprint
import argparse
from pymongo import MongoClient
import boto3
import botocore
import requests
import urllib.parse


from collections import namedtuple

base_url = "https://maps.googleapis.com/maps/api/geocode/json"

def geocode(address, map_key):
    place=urllib.parse.quote(address.replace(" ", "+"), safe="+")

    url = "{url}?address={addr}&key={key}".format(
        url=base_url,
        addr=place,
        key=map_key)

    r = requests.get(url)

    return r.json()


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


def f(s):
    return s.strip()


def load_rms(fp, map_key):
    r = csv.reader(fp, delimiter="\t")
    hdr = next(r)
    hdr = hdr[:14]
    hdr.append("place_id")
    RMS = namedtuple('RMS', hdr)
    
    results = []

    for rec in r:
        m = RMS._make(list(map(f, rec[:14])) + [""])

        addr = " ".join([m.Number,
                         m.Dir,
                         m.Street,
                         m.Type.title()])

        if m.Suite:
            addr += " #%s" % m.Suite
        
        address = "%s, %s, %s" % (addr, "Urbana", "IL")
        r = geocode(address, map_key)
        pprint.pprint(r["results"])

        try:
            place_id = r["results"][0]["place_id"]
            print("worked", address, place_id)
        except:
            print("failed", address)
            pprint.pprint(r)
            place_id = ""

        # now that we have the place id, rebuild tuple
        m = RMS._make(rec[:14] + [place_id])
        results.append(m)

    return results


def read_rms(fp, map_key):
    r = csv.reader(fp, delimiter="\t")
    hdr = next(r)
    hdr = hdr[:14]
    RMS = namedtuple('RMS', hdr)
    
    results = []

    for rec in r:
        m = RMS._make(list(map(f, rec[:14])))

        addr = " ".join([m.Number,
                         m.Dir,
                         m.Street,
                         m.Type.title()])

        if m.Suite:
            addr += " #%s" % m.Suite
        
        address = "%s, %s, %s" % (addr, "Urbana", "IL")
        #r = geocode(address, map_key)
        yield m, addr

    return


def find_matching_rms(location, rms_data, args, map_key):
    if "address" not in location:
        return

    if "address1" not in location["address"]:
        return

    print("------------------------------------------------------------")
    addr1 = location["address"]["address1"]
    name = location["name"]

    print(name, location["address"])

    for rms in rms_data:
        if not rms.Occup_Id:
            continue

        if not rms.place_id:
            continue

        address = ", ".join([
            addr1,
            location["city"],
            location["state"],
            location["zip"]])

        print("about to geocode", address)
        r = geocode(address, map_key)
        pprint.pprint(r)

        try:
            place_id = r["results"][0]["place_id"]
        except:
            place_id = ""

        #name[0].lower() == rms.Name[0].lower()):
        if place_id == rms.place_id:
            pprint.pprint(rms)


        #if (addr1.startswith(rms.Number) and
        #    rms.Street in addr1 and True):
        #    pprint.pprint(rms)

    print()

    return


def load_locations(db, coll):
    results = []

    for row in db[coll].find():
        if "address" not in row or "address1" not in row["address"]:
            continue

        results.append(row)

    return results


def find_match(rms, loc):
    addr = loc["address"]["address1"].lower()

    if rms.Number and not addr.startswith(rms.Number):
        return False

    # if address has a street number but RMS does not, fail
    if re.match(r"^\d+", addr) and not rms.Number:
        return False

    if rms.Street.lower() not in addr:
        return False

    if rms.Dir and rms.Dir.lower() not in addr:
        return False

    return True


def update_loc(locations, i, rms):
    if "building" not in locations[i]:
        locations[i]["building"] = {}

    if "notes" not in locations[i]["building"]:
        locations[i]["building"]["notes"] = ""

    flds = ["NOTE", rms.Occup_Id]

    for f in rms.FPP, rms.LIQ, rms.HAZ, rms.U116, rms.UIUC:
        if f:
            flds.append(f)

    new_note = ",".join(flds)

    if locations[i]["building"]["notes"]:
        locations[i]["building"]["notes"] += ("\n" + new_note)
    else:
        locations[i]["building"]["notes"] = new_note

    return
    

def any_notes(rms):
    if rms.FPP or rms.LIQ or rms.HAZ or rms.U116 or rms.UIUC:
        return True
    else:
        return False


def print_rms(rms):
    orec = [rms.Occup_Id,
            rms.Name,
            rms.Number,
            rms.Dir,
            rms.Street,
            rms.Type,
            rms.Suite,
            rms.FPP,
            rms.LIQ,
            rms.HAZ,
            rms.U116,
            rms.UIUC]

    return orec


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="check for broken links")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("slug")
    parser.add_argument("--limit", type=int)

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")
    map_key = get_parm(session, "GOOGLE_MAP_API_KEY")

    client = MongoClient(mongo_uri)
    db = client.FlowMSP

    # rms_data = load_rms(sys.stdin, map_key)
    coll = "%s.Location" % args.slug
    locations = load_locations(db, coll)
    rmsnum = 0
    updates = set()

    w = csv.writer(open("rms_nomatch.csv", "w"), lineterminator="\n")
    w.writerow(["Occup_Id",
                "Name",
                "Number",
                "Dir",
                "Street",
                "Type",
                "Suite",
                "FPP",
                "LIQ",
                "HAZ",
                "U116",
                "UIUC"])

    for rms, addr in read_rms(sys.stdin, map_key):
        rmsnum += 1

        # print("---------------------------%d ---------------------------------" % rmsnum)

        matches = []

        for i, loc in enumerate(locations):
            if find_match(rms, loc):
                update_loc(locations, i, rms)
                updates.add(i)
                matches.append(loc)

        if not matches:
            w.writerow(print_rms(rms))

    for i, loc in enumerate(locations):
        if i in updates:
            db[coll].replace_one({"_id": loc["_id"]}, loc)

            print(loc["_id"])
            print(locations[i]["address"])
            print(locations[i]["building"]["notes"])
            print()


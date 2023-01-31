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


ohdr = ["Polygon", "Name", "Street Address", "Street Address2", "City", 
        "State", "Zip code", "Roof Area (Sq. Ft)", "Occupancy Type",
        "Construction Type", "Roof Type", "Roof Construction",
        "Roof Material", "Normal Population", "Sprinklered", "Stand Pipe",
        "Fire Alarm", "Hours of Operation", "Owner Contact", "Owner Phone",
        "Notes", "Storey Above", "Storey Below"]

# https://developer.here.com/documentation/batch-geocoder/topics/endpoints.html

#    "0": "Undetermined",

construction_type = {
    "1": "Type IA - Fire Resistive, Non-combustible",
    "2": "Type IV - Heavy Timber",
    "3": "Type IIA - Protective, Non-combustible",
    "4": "Type IIB - Unprotective, Non-combustible",
    "5": "Type IIIA - Protected Ordinary",
    "6": "Type IIIB - Unprotected Ordinary",
    "7": "Type VA - Protected Combustible",
    "8": "Type VB - Unprotected Combustible",
    "9": "Not Classified"
}

"Built-Up",
"Composition Shingles",
"Membrane - Plastic elastomer",
"Membrane - Synthetic elastomer",
"Metal",
"Metal - Corrugated",
"Metal - Shake",
"Metal - Standing Seam",
"Roof Covering Not Class",
"Roof Covering Undetermined/Not Reported",
"Shingle - Asphalt / Composition",
"Slate - Composition",
"Slate - Natural",
"Structure Without Roof",
"Tile - Clay",
"Tile - Concrete",
"Wood - Shingle/Shake",
"Wood Shakes/Shingles (Treated)",
"Wood Shakes/Shingles (Untreated)"


#    "0": "Roof Covering Undetermined/Not Reported",
roof_covering = {
    "1": "Tile (clay, cement, slate, etc.)",
    "2": "Composition Shingles",
    "3": "Wood Shakes/Shingles (Treated)",
    "4": "Wood Shakes/Shingles (Untreated)",
    "6": "Metal",
    "7": "Built-Up",
    "8": "Structure Without Roof",
    "9": "Roof Covering Not Class"
}

app_id="WaGiLAKzJQzySaXFJm9o"
app_code="bO4lKri2pJuiCxnRuD0-KQ"
base_url = "https://batch.geocoder.api.here.com/6.2/jobs"

def parse_xml(text, tag):
    root = ET.fromstring(text)

    for e in root.iter(tag):
        #print(e.tag, e.text)
        return e.text


# recId|searchText|country
# 0003|425 W Randolph St Chicago IL 60606|USA
# 0004|One Main Street Cambridge MA 02142|USA
# 0005|200 S Mathilda Ave Sunnyvale CA 94086|USA
def submit_batch(records):
    url = "{b}?app_id={app_id}&app_code={app_code}".format(
        b=base_url,
        app_id=app_id,
        app_code=app_code
    )

    url += "&indelim=%7C&outdelim=%7C&action=run"

    outcols = ["displayLatitude", "displayLongitude", "locationLabel",
               "houseNumber", "street", "district", "city", "postalCode",
               "county", "state", "country"]

    url += "&outcols={cols}".format(cols=",".join(outcols))
    url += "&outputcombined=false"

    body = ["|".join(r) for r in records]
    data = "\n".join(body)

    # print(url)
    r = requests.post(url, data=data)

    return r


def get_status(request_id):
    url = "{b}/{r}?action=status&app_code={c}&app_id={app_id}".format(
        b=base_url,
        r=request_id,
        c=app_code,
        app_id=app_id)

    # print(url)

    r = requests.get(url)

    if r.ok:
        return parse_xml(r.text, "Status")


def get_results(request_id, output="result"):
    url = "{b}/{r}/{output}?app_id={app_id}&app_code={app_code}".format(
        b=base_url,
        output=output,
        r=request_id,
        app_id=app_id,
        app_code=app_code)
        
    url += "&outputcompressed=false"

    # print(url)
    r = requests.get(url)

    if r.ok:
        return r


def make_address_line(irec):
    return " ".join([irec["number"],
                     irec["st_prefix"],
                     irec["street"],
                     irec["st_type"],
                     irec["st_suffix"]]).strip()


def make_address_line2(irec):
    return " ".join([irec["addr_2"],
                     irec["apt_room"]]).strip()


def make_contact(irec):
    return " ".join([irec["first_name"],
                     irec["last_name"]]).strip()


def make_phone(irec):
    return " ".join([irec["phone"],
                     irec["ext"]]).strip()


def make_note(irec):
    existing_notes = irec.get("Notes", irec.get("notes", ""))

    if existing_notes:
        existing_notes += "\n"

    desc = " - ".join([irec["prop_use"],
                       irec["property"]]).strip()

    occ_type = " ".join(["Occupancy Type:", desc]).strip()

    return existing_notes + occ_type


def reformat_geocoded_results(records):
    hdr = records[0].split("|")
    Geo = namedtuple("GEO", hdr)
    results = []

    for r in records[1:-1]:
        rec = r.split("|")
        geo = Geo._make(rec)

        if geo.SeqNumber != '1':
            # print("skipped", geo, file=sys.stderr)
            continue
        results.append(geo._asdict())

    return results


def batch_geocode(records, nap_time):
    hdr = ["searchText", "country"]
    batch = [hdr]

    for r in records:
        address_line = "{addr1} {city} {state} {zip}".format(
            addr1=r["Street Address"],
            city=r["City"],
            state=r["State"],
            zip=r["Zip code"])

        batch.append([address_line, "USA"])

    print("inrecs=%d" % len(batch), file=sys.stderr)
    r = submit_batch(batch)

    if not r.ok:
        print("submit failed", file=sys.stderr)
        print(r.text, file=sys.stderr)
        return
    
    request_id = parse_xml(r.text, "RequestId")

    print("request_id", request_id, file=sys.stderr)
    status = get_status(request_id)

    while status != "completed":
        print("status", status, file=sys.stderr)
        time.sleep(args.nap_time)
        status = get_status(request_id)

    print("status", status, file=sys.stderr)

    r = get_results(request_id)

    results = r.text.split("\n")

    return reformat_geocoded_results(results)


def reformat_records(fp, limit):
    r = csv.DictReader(fp)
    records = []

    # TODO: geocode addresses in batch
    for recnum, irec in enumerate(r):
        if limit and recnum >= limit:
            break

        orec = {}

        # TODO: look for matching polygons
        orec["Polygon"] = ""
        orec["Name"] = irec["occ_name"]

        # number, st_prefix, street, st_type, st_suffix, apt_room
        orec["Street Address"] = make_address_line(irec)
        orec["Street Address2"] = make_address_line2(irec)
        orec["City"] = irec["city"]
        orec["State"] = irec["state"]
        orec["Zip code"] = irec["zip"]

        orec["Construction Type"] = construction_type.get(irec["construct"], "")
        orec["Roof Material"] = roof_covering.get(irec["roof_cover"], "")

        # first_name, last_name
        orec["Owner Contact"] = make_contact(irec)

        # phone, ext
        orec["Owner Phone"] = make_phone(irec)
        
        # property_class, prop_use, property
        orec["Notes"] = make_note(irec)
        orec["Storey Above"] = irec["floors_above"]
        orec["Storey Below"] = irec["floors_below"]
        records.append(orec)
    
    return records


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

    return db.ms_geodata.find_one(query)


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


def get_polygon_string(outline):
    points = []

    for lon, lat in outline["coordinates"][0]:
        p = ":".join([str(lat), str(lon)])
        points.append(p)

    return "|".join(points)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="load RMS data")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("csv_file")
    parser.add_argument("--request-id")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--nap-time", type=int, default=30)

    args = parser.parse_args()
    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")
    client = MongoClient(mongo_uri)
    db = client.FlowMSP

    records = reformat_records(open(args.csv_file), args.limit)

    # submit these for batch geocoding
    result = batch_geocode(records, args.nap_time)

    w = csv.DictWriter(sys.stdout, ohdr, lineterminator="\n")
    w.writeheader()

    print("records", len(records), file=sys.stderr)
    print("result", len(result), file=sys.stderr)

    for r, b in zip(records, result):
        lat = float(b["displayLatitude"])
        lon = float(b["displayLongitude"])

        outline = get_outline(db, lat, lon)

        if outline and "geometry" in outline:
            r["Polygon"] = get_polygon_string(outline["geometry"])

        w.writerow(r)
    

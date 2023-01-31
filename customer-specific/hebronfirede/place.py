#!/usr/bin/env python3

import os
import sys
import glob
import urllib.parse
import argparse
import pprint

import requests
import geojson
import shapely
import shapely.geometry


def places(lat, lon, place=None):
    base_url = "https://places.cit.api.here.com/places/v1/autosuggest"
    url = "{b}?at={lat},{lon}&app_id={app_id}&app_code={app_code}".format(
        b=base_url,
        lat=lat,
        lon=lon,
        app_id="WaGiLAKzJQzySaXFJm9o",
        app_code="bO4lKri2pJuiCxnRuD0-KQ")

    if place:
        url += "&q={place}".format(place=place)

    r = requests.get(url)

    return r
    

def places(lat, lon, place=None):
    base_url = "https://places.cit.api.here.com/places/v1/autosuggest"
    url = "{b}?at={lat},{lon}&app_id={app_id}&app_code={app_code}".format(
        b=base_url,
        lat=lat,
        lon=lon,
        app_id="WaGiLAKzJQzySaXFJm9o",
        app_code="bO4lKri2pJuiCxnRuD0-KQ")

    if place:
        url += "&q={place}".format(place=place)

    r = requests.get(url)

    return r
    

def geocode(address, city, state):
    base_url="https://geocoder.api.here.com/6.2/geocode.json"

    url = "{b}?app_id={app_id}&app_code={app_code}&searchtext={place}&city={city}&state={state}&country=usa".format(
        b=base_url,
        app_id="WaGiLAKzJQzySaXFJm9o",
        app_code="bO4lKri2pJuiCxnRuD0-KQ",
        place=urllib.parse.quote(address.replace(" ", "+"), safe="+"),
        city=urllib.parse.quote(city.replace(" ", "+"), safe="+"),
        state=state)

    r = requests.get(url)

    if r.ok:
        return r.json()


def within_polygon(loc, polygon):
    # polygon = shapely.geometry.asShape(feature["geometry"])

    return polygon.contains(loc["point"])


def load_addresses(wdir):
    files = glob.glob("%s/*.pdf" % wdir)
    records = []

    for f in files:
        name = os.path.basename(f)
        name = name.replace(".pdf", "")

        business, address_line = name.rsplit(" - ", 1)

        addr = address_line.split()
        state = addr.pop(-1)
        city = addr.pop(-1)
        addr1 = " ".join(addr)
        zipcode = "63801"

        gc = geocode(addr1, city, state)

        rec = {
            "name": business,
            "address1": addr1,
            "city": city,
            "state": state,
            "zipcode": zipcode,
            "geocode": gc,
            "matches": 0
        }

        try:
            lat = gc["Response"]["View"][0]["Result"][0]["Location"]["DisplayPosition"]["Latitude"]
            lon = gc["Response"]["View"][0]["Result"][0]["Location"]["DisplayPosition"]["Longitude"]
            rec["point"] = shapely.geometry.Point(lon, lat)
        except:
            print("------------------------------------------------------------")
            print("geocode failed")
            pprint.pprint(rec)
            pprint.pprint(gc)

        records.append(rec)
    

    return records
    

def compare_record(a, f):
    props = f["properties"]

    if a.city != props.get("city"):
        return False

    if a.state != props["state"]:
        return False

    if a.zipcode != props["postalcode"]:
        return False

    if props.get("estimatedHouseNumber", "N/A") not in a.addr1:
        return False

    if props["street"].lower() not in a.addr1.lower():
        return False

    a.matches += 1

    return True
        

def print_feature(f):
    props = f["properties"]
    
    return [props.get(k, "n/a") for k in ["estimatedHouseNumber", "street", "city", "state", "postalcode"]]


if __name__ == "__main__":
    addr = load_addresses("/Users/doug/Desktop/sikeston/addresses")

    jdata = geojson.load(open(sys.argv[1]))
    print("Features", len(jdata["features"]), file=sys.stderr)

    for f in jdata["features"]:
        # pprint.pprint(f["properties"])
        #print(print_feature(f))

        polygon = shapely.geometry.asShape(f["geometry"])

        for a in addr:
            if "point" not in a:
                continue

            if within_polygon(a, polygon):
                print("------------------------------------------------------------")
                print(a)
                pprint.pprint(f)
                a["matches"] += 1

    print("------------------------------------------------------------")
    for a in addr:
        print(a)



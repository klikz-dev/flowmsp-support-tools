#!/usr/bin/env python3

import os
import sys
import glob
import urllib.parse
import argparse
import pprint
import random
import requests
import json
import geojson

from smartystreets_python_sdk import StaticCredentials, exceptions, ClientBuilder
from smartystreets_python_sdk.us_street import Lookup

def standardize_address_cloud_post(addresses):
    auth_id = "8a3806c3-f12e-7ad8-876a-e080fcbf3536"
    auth_token = "OWkjJohjmBC9Abnyl5zk"
    url = "https://us-street.api.smartystreets.com/street-address?auth-id={auth_id}&auth-token={token}".format(
        auth_id=auth_id, token=auth_token)

    #url += "&street={addr}&city={city}&state={st}&candidates=10".format(
    #    addr=addr, city=city, st=state)

    r = requests.post(url, data=json.dumps(addresses))

    if r.ok:
        return r.json()


def standardize_address_cloud(address1, city, state):
    addr = urllib.parse.quote(address1.replace(" ", "+"), safe="+")
    city = urllib.parse.quote(city.replace(" ", "+"), safe="+")
    auth_id = "8a3806c3-f12e-7ad8-876a-e080fcbf3536"
    auth_token = "OWkjJohjmBC9Abnyl5zk"
    url = "https://us-street.api.smartystreets.com/street-address?auth-id={auth_id}&auth-token={token}".format(
        auth_id=auth_id, token=auth_token)

    url += "&street={addr}&city={city}&state={st}&candidates=10".format(
        addr=addr, city=city, st=state)

    r = requests.get(url)

    if r.ok:
        return r.json()


def standardize_address(client, rec):
    lookup = Lookup()
    lookup.input_id = "24601"  # Optional ID from your system
    lookup.addressee = "John Doe"
    lookup.street = rec["address1"]
    lookup.city = rec["city"]
    lookup.state = rec["state"]
    lookup.zipcode = rec["zipcode"]
    lookup.candidates = 3
    lookup.match = "Invalid"  # "invalid" is the most permissive match

    try:
        client.send_lookup(lookup)
    except exceptions.SmartyException as err:
        print(err)
        return

    result = lookup.result

    if not result:
        print("No candidates. This means the address is not valid.")
        return

    first_candidate = result[0]

    print("Address is valid. (There is at least one candidate)\n")
    print("ZIP Code: " + first_candidate.components.zipcode)
    print("County: " + first_candidate.metadata.county_name)
    print("Latitude: {}".format(first_candidate.metadata.latitude))
    print("Longitude: {}".format(first_candidate.metadata.longitude))

    return result


def geocode(address, city, state):
    base_url="https://geocoder.api.here.com/6.2/geocode.json"

    url = "{b}?app_id={app_id}&app_code={app_code}&searchtext={place}&city={city}&state={state}&country=usa".format(
        b=base_url,
        place=urllib.parse.quote(address.replace(" ", "+"), safe="+"),
        city=urllib.parse.quote(city.replace(" ", "+"), safe="+"),
        state=state)

    r = requests.get(url)

    if r.ok:
        return r.json()


def create_client():
    auth_id = "8a3806c3-f12e-7ad8-876a-e080fcbf3536"
    auth_token = "OWkjJohjmBC9Abnyl5zk"

    credentials = StaticCredentials(auth_id, auth_token)
    client = ClientBuilder(credentials).build_us_street_api_client()

    return client


def make_batch_record(f):
    props = f["properties"]
    addr1 = "{num} {street}".format(
        num=props.get("estimatedHouseNumber", ""),
        street=props.get("street", ""))

    return {
        "input_id": f["id"],
        "street": addr1,
        "city": props.get("city", ""),
        "state": props.get("state", ""),
        "zipcode": props.get("postalcode", ""),
        "candidates": 10
    }


def make_batch(features):
    return [make_batch_record(f) for f in features]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="run address standardization on a geojson dataset")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--batch-size", type=int, default=10)
    parser.add_argument("--shuffle", action="store_true")

    args = parser.parse_args()
    data = geojson.load(sys.stdin)
    batch = []
    results = []
    features = {}

    if args.shuffle:
        g = random.Random()
        g.shuffle(data["features"])

    for fnum, f in enumerate(data["features"]):
        if args.limit and fnum >= args.limit:
            break

        if len(batch) == args.batch_size:
            print(fnum, "sending batch", file=sys.stderr)
            if args.verbose:
                for i, b in enumerate(batch):
                    print(i, b, file=sys.stderr)

            r = standardize_address_cloud_post(batch)

            if args.verbose:
                print("%d results returned" % len(r), file=sys.stderr)

            for ad in r:
                if args.verbose:
                    print(ad, file=sys.stderr)
                results.append(ad)

            batch = []

        b = make_batch_record(f)
        batch.append(b)
        features[f["id"]] = f

    if batch:
        print(fnum, "sending batch", file=sys.stderr)
        if args.verbose:
            for i, b in enumerate(batch):
                print(i, b, file=sys.stderr)

        r = standardize_address_cloud_post(batch)

        if args.verbose:
            print("%d results returned" % len(r), file=sys.stderr)

        for ad in r:
            if args.verbose:
                print(ad, file=sys.stderr)
            results.append(ad)
        
    print("%d records returned from address standardization" % len(results),
          file=sys.stderr)

    # match up results to input and write out
    for r in results:
        try:
            id = r["input_id"]
        except:
            print(r, file=sys.stderr)
            continue
            
        features[id]["properties"]["standardized_address"] = r

    # turn into a list
    features_as_list = [features[f] for f in features]
    collection = geojson.FeatureCollection(features_as_list)
    geojson.dump(collection, sys.stdout)


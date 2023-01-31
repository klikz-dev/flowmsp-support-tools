#!/usr/bin/env python3

import sys
import pprint
import time
import argparse
import requests
import boto3
from pymongo import MongoClient


def get_link(links, rel):
    for link in links:
        if link["rel"] == rel:
            return link["href"]


def get_db(profile):
    session = boto3.session.Session(profile_name=profile)
    mongo_uri = get_parm(session, "mongo_uri")
    client = MongoClient(mongo_uri)
    db = client.FlowMSP
    return db


def get_location(db, slug, id):
    row = db["%s.Location" % slug].find_one({"_id": id})

    return row


def update_location(db, slug, row):
    id = row["_id"]
    r = db["%s.Location" % slug].replace_one({"_id": id}, row)
    return


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


def get_data(db, url, headers):
    r = requests.get(url, headers=headers)

    if r.ok:
        result = r.json()
        return result.get("data", [])


def preplan(db, loc, headers):
    preplan_url = get_link(loc["links"], "preplan")

    r = requests.get(preplan_url, headers=headers)

    if r.ok:
        return r.json()

    return 


def do_preplan(db, loc, slug, headers):
    plan = preplan(db, loc, headers)
    row = get_location(db, slug, loc["id"])

    pprint.pprint(plan)

    row["roofArea"] = plan["roofArea"]
    row["requiredFlow"] = plan["requiredFlow"]

    if "hydrants" in plan:
        row["hydrants"] = [h["id"] for h in plan["hydrants"]]
    elif "hydrants" in row:
        row.pop("hydrants")

    update_location(db, slug, row)

    return row, plan


def assign_flow_rates(db, loc, slug, headers):
    plan = preplan(db, loc, headers)
    row = get_location(db, slug, loc["id"])

    # requiredFlow may not be exactly correct, check
    row["roofArea"] = plan["roofArea"]
    row["requiredFlow"] = plan["roofArea"] // 3

    update_location(db, slug, row)

    return row, plan


def assign_hydrants(db, loc, slug, headers):
    plan = preplan(db, loc, headers)
    row = get_location(db, slug, loc["id"])

    if "hydrants" in plan:
        row["hydrants"] = [h["id"] for h in plan["hydrants"]]
        update_location(db, slug, row)
    else:
        print(locnum, "no hydrants selected", loc["id"], loc["address"])

    return row, plan


def login(hostname, userid, pw):
    # url = "http://{hostname}/api/auth/token".format(hostname=hostname)
    url = "{hostname}/api/auth/token".format(hostname=hostname)

    print(userid, pw, file=sys.stderr)
    r = requests.post(url, auth=(userid, pw))

    if r.ok:
        auth = r.json()
    else:
        print(r.status_code, file=sys.stderr)
        print(r.text, file=sys.stderr)
        raise SystemExit(1)

    headers = {"Authorization": "{token_type} {access_token}".format(
        token_type=auth["tokenType"],
        access_token=auth["accessToken"])
    }

    return auth, headers


def has_preplan(row):
    if row.get("roofArea", 0) == 0:
        return False
    
    if row.get("requiredFlow", 0) == 0:
        return False

    if len(row.get("hydrants", [])) == 0:
        return False

    return True
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="recalculate pre-plans")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("userid")
    parser.add_argument("password")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--batch-size", type=int, default=25)
    parser.add_argument("--nap-time", type=int)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--id")

    args = parser.parse_args()
    db = get_db(args.profile)

    if args.profile == "flowmsp-prod":
        api_server = "https://app.flowmsp.com"
    else:
        api_server = "https://test.flowmsp.com"

    auth, headers = login(api_server, args.userid, args.password)

    customer_url = get_link(auth["links"], "customer")

    r = requests.get(customer_url, headers=headers)

    if r.ok:
        customer = r.json()

    print("Name: %s" % customer["name"])
    print("Slug: %s" % customer["slug"])
    print("Addr: %s" % customer["address"]["address1"])
    print("City: %s, %s  %s" %
          (customer["address"]["city"],
          customer["address"]["state"],
          customer["address"]["zip"]))
    print()

    location_url = get_link(customer["links"], "locations")
    hydrants_url = get_link(customer["links"], "hydrants")

    locations = get_data(db, location_url, headers)
    hydrants = get_data(db, hydrants_url, headers)

    print("Locations: %d" % len(locations))
    print(" Hydrants: %d" % len(hydrants))

    slug = customer["slug"]

    for locnum, loc in enumerate(locations):
        if args.id and loc["id"] != args.id:
            continue

        if args.limit and locnum >= args.limit:
            break

        if not args.force and has_preplan(loc):
            continue

        print("------------------------------------------------------------")
        hydrants = [h.get("id", "") for h in loc.get("hydrants", [])]
        print(locnum,
              loc["id"],
              loc["address"],
              loc.get("roofArea", ""),
              loc.get("requiredFlow", ""),
              hydrants)

        row, plan = do_preplan(db, loc, slug, headers)

        pprint.pprint(plan)

        if args.nap_time:
            time.sleep(args.nap_time)
    

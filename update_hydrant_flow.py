#!/usr/bin/env python3

import os
import sys
import pprint
import argparse
import csv
from pymongo import MongoClient
import boto3
import botocore


def_colors = {'rangePinColors': [{'high': 500,
                                  'label': '0 to less than 500 GPM',
                                  'low': 0,
                                  'pinColor': 'RED'},
                                 {'high': 1000,
                                  'label': '500 to less than 1000 GPM',
                                  'low': 500,
                                  'pinColor': 'ORANGE'},
                                 {'high': 1500,
                                  'label': '1000 to less than 1500 GPM',
                                  'low': 1000,
                                  'pinColor': 'GREEN'},
                                 {'high': 100000,
                                  'label': '1500+ GPM',
                                  'low': 1500,
                                  'pinColor': 'BLUE'}],
              'unknownPinColor': {'high': 0,
                                  'label': 'Unknown',
                                  'low': 0,
                                  'pinColor': 'YELLOW'}
}

def set_flow_range(flow, pin_legend):
    for p in pin_legend["rangePinColors"]:
        if flow < p["high"]:
            return {
                "label": p["label"],
                "pinColor": p["pinColor"]
            }

    return {
        "label": pin_legend["unknownPinColor"]["label"],
        "pinColor": pin_legend["unknownPinColor"]["pinColor"]
    }


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


def get_customer(db, slug):
    return db.Customer.find_one({"slug": slug})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="update hydrant flow rates")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("slug")
    parser.add_argument("csv_file")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--hydrant-id", default="HYDRANT_ID")
    parser.add_argument("--flow")
    parser.add_argument("--notes")
    parser.add_argument("--dry-run", action="store_true")

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")

    PGM = os.path.basename(sys.argv[0])

    client = MongoClient(mongo_uri)
    db = client.FlowMSP
    coll = db["%s.Hydrant" % args.slug]
    c = get_customer(db, args.slug)

    rows = notfound = updated = skipped = 0

    r = csv.DictReader(open(args.csv_file))

    for rec in r:
        rows += 1

        if args.limit and rows >= args.limit:
            break

        # no sense continuing if flow value is empty
        if rec[args.flow] == "":
            skipped += 1
            continue

        hydrant_id = rec[args.hydrant_id]

        row = coll.find_one({"hydrantId": hydrant_id})

        if not row:
            notfound += 1
            continue

        flow = int(rec[args.flow].replace(",", ""))
        row["flow"] = flow
        row["flowRange"] = set_flow_range(flow, c.get("pinLegend", def_colors))


        if args.notes:
            note = "%s=%s" % (args.notes, rec[args.notes])

            if row.get("notes", ""):
                row["notes"] += ";%s" % note
            else:
                row["notes"] = note

        print("------------------------------------------------------------")
        pprint.pprint(rec)
        pprint.pprint(row)
        print()

        if args.dry_run:
            continue

        r = coll.replace_one({"_id": row["_id"]}, row)
        updated += 1

    print("rows=%d skipped=%d notfound=%d updated=%d" %
          (rows, skipped, notfound, updated), file=sys.stderr)

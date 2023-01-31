#!/usr/bin/env python3

import sys
import csv
import time
import argparse
import pprint
from pymongo import MongoClient
import boto3
import botocore


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


def get_db(profile):
    session = boto3.session.Session(profile_name=profile)
    mongo_uri = get_parm(session, "mongo_uri")
    client = MongoClient(mongo_uri)

    return client.FlowMSP


def count_hydrants(db, table, counts):
    rows = 0

    for row in db[table].find():
        rows += 1

        if "createdOn" in row:
            dt = row["createdOn"].strftime("%Y-%m-%d")

            if dt not in counts:
                counts[dt] = 0
            counts[dt] += 1
        else:
            counts["n/a"] += 1

    return rows


def count_s3_images(url, counts):
    _, _, _, bucket, key = url.split("/", 4)

    s3 = boto3.client("s3")

    try:
        o = s3.get_object(Bucket=bucket, Key=key)
    except:
        print("error", url, file=sys.stderr)
        return

    if "LastModified" in o:
        dt = o["LastModified"].strftime("%Y-%m-%d")

        if dt not in counts:
            counts[dt] = 0

        counts[dt] += 1

    return


def count_locations(db, table, counts):
    rows = 0

    for row in db[table].find():
        rows += 1

        if "createdOn" in row:
            dt = row["createdOn"].strftime("%Y-%m-%d")

            if dt not in counts:
                counts[dt] = 0
            counts[dt] += 1
        else:
            counts["n/a"] += 1

    return rows


def count_images(db, table, counts):
    rows = 0

    for row in db[table].find():
        rows += 1

        if "images" in row:
            for i in row["images"]:
                if "href" in i:
                    counts.append(i["href"])

    return rows


def count_dispatches(db, table, counts):
    rows = 0

    for row in db[table].find():
        rows += 1

        if "sequence" in row:
            tstamp = time.localtime(row["sequence"]/1000)
            dt = time.strftime("%Y-%m-%d", tstamp)

            if dt not in counts:
                counts[dt] = 0
            counts[dt] += 1
        else:
            counts["n/a"] += 1

    return rows


# structures
# hydrants
# images
# dispatches
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="check for broken links")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("--limit", type=int)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("collection_type", nargs="+",
                        choices=["locations", "hydrants", "images", "dispatches"])
 
    args = parser.parse_args()

    db = get_db(args.profile)
    counts = {
        "locations": {"n/a": 0},
        "hydrants": {"n/a": 0},
        "images": { "n/a": 0},
        "dispatches": {"n/a": 0},
    }

    images = []
    tables = 0

    for rownum, row in enumerate(db.list_collections()):
        if args.limit and rownum >= args.limit:
            break

        if row["name"].endswith(".Hydrant"):
            if "hydrants" in args.collection_type:
                tables += 1
                rows = count_hydrants(db, row["name"], counts["hydrants"])

                if args.verbose:
                    print("%d: %s: rows=%d" % (tables, row["name"], rows), file=sys.stderr)

        elif row["name"].endswith(".User"):
            continue

        elif row["name"].endswith(".Location"):
            if "locations" in args.collection_type:
                tables += 1
                rows = count_locations(db, row["name"], counts["locations"])

                if args.verbose:
                    print("%d: %s: rows=%d" % (tables, row["name"], rows), file=sys.stderr)

            if "images" in args.collection_type:
                tables += 1
                rows = count_images(db, row["name"], images)

                if args.verbose:
                    print("%d: %s: rows=%d" % (tables, row["name"], rows), file=sys.stderr)

        elif row["name"].endswith(".MsgReceiver"):
            if "dispatches" in args.collection_type:
                tables += 1
                rows = count_dispatches(db, row["name"], counts["dispatches"])

                if args.verbose:
                    print("%d: %s: rows=%d" % (tables, row["name"], rows), file=sys.stderr)

        else:
            print("other table name", row["name"], file=sys.stderr)
            continue

    print("there are %d images" % len(images), file=sys.stderr)
    c = len(images) // 10

    # print progress about every 10%
    for i, img in enumerate(images):
        if (i % c) == 0:
            print("i=%d" % i, file=sys.stderr)

        count_s3_images(img, counts["images"])

    hdr = ["dt", "num", "type"]
    w = csv.DictWriter(sys.stdout, hdr, lineterminator="\n")
    w.writeheader()

    for t in counts:
        for dt in counts[t]:
            orec = {
                "dt": dt,
                "num": counts[t][dt],
                "type": t
            }
            w.writerow(orec)

    # pprint.pprint(counts)

    raise SystemExit(0)

#if row["name"].endswith(".User"):
#            process_cuser_table(db, row["name"], args.username, args.role, args.find_orphans, customers)

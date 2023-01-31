#!/usr/bin/env python3

import os
import sys
import argparse
import boto3
import json
import pprint
import base64
import re
from pymongo import MongoClient


def get_ids(slug, key):
    base, ext = os.path.splitext(key)

    image_id = os.path.basename(base)
    location_id = os.path.basename(os.path.dirname(base))

    return slug, location_id, image_id


def connect(profile):
    if profile == "flowmsp-prod":
        return MongoClient(prod_url)
    else:
        return MongoClient(dev_url)


def run_function(session, slug, location_id, image_id):
    c = session.client("lambda")

    payload = {
        "slug": slug,
        "locationId": location_id,
        "imageId": image_id
    }

    r = c.invoke(
        FunctionName="createAnnotatedThumbnail",
        InvocationType="RequestResponse",
        Payload=json.dumps(payload)
    )

    return r


def print_info(image, keys):
    for k in keys:
        print("  %s=<%s>" % (k, image[k]))

    return


def find_missing_thumbnails(location):
    missing = []

    if "images" not in location:
        return missing

    for i in location["images"]:
        if i.get("hrefThumbnail", "NoImage") == "NoImage":
            missing.append(i)

    return missing


def update_record(x):
    return


def get_customers(db):
    return [row["slug"] for row in db.Customer.find()]


def call_function(profile, slug, location_id, image_id, image, function):
    if args.function == "createAnnotatedImageAndThumbnail":
        payload = {
            "slug": args.slug,
            "locationId": args.location_id,
            "imageId": args.image_id,
            "annotationMetadataJSON": json.loads(image["annotationMetadata"])
            }

    elif args.function in ("createdAnnotatedImage", "createAnnotatedImage_new"):
        payload = {
            "slug": args.slug,
            "locationId": args.location_id,
            "imageId": args.image_id,
            "annotationMetadataJSON": json.loads(image["annotationMetadata"])
            }

    elif args.function in ("createAnnotatedThumbnail", "createAnnotatedThumbnail_new"):
        payload = {
            "slug": args.slug,
            "locationId": args.location_id,
            "imageId": args.image_id,
            }

    elif args.function == "resize-image":
        # url = image["href"]
        url = image["hrefOriginal"]

        _, _, _, b, new_key = url.split("/", 4)

        #new_key = "originals/%s" % k
        #new_key = new_key.replace("-1024h", "")
        #new_key = re.sub(r"-(1024|300)h", "", new_key)

        print("url", url)
        print("b", b)
        #print("k1", k1)
        #print("k", k)
        print("new_key", new_key)

        payload = {
            "Records": [
                {
                    "s3": {
                        "configurationId": "testConfigRule",
                        "object": {
                            "eTag": "c1e0946156638cad4c6ac699e90f54d2",
                            "sequencer": "0A1B2C3D4E5F678901",
                            "key": new_key,
                            "size": 1024
                        },
                        "bucket": {
                            "arn": "arn:aws:s3:::tuple-source",
                            "name": b,
                            "ownerIdentity": {
                                "principalId": "tuple-demo"
                            }
                        }
                    },
                    "eventName": "ObjectCreated:Put"
                }
            ]
        }
    else:
        print("unsupported lambda function: %s" % args.function,
              file=sys.stderr)
        raise SystemExit(1)

    print("------------------------------------------------------------")
    pprint.pprint(payload)
    print("------------------------------------------------------------")

    sess = boto3.session.Session(profile_name=profile)
    c = sess.client("lambda")

    data = json.dumps(payload).encode()

    runtype = "RequestResponse"
    # runtype = "DryRun"
    r = c.invoke(FunctionName=function,
                 InvocationType="RequestResponse",
                 LogType="Tail",
                 Payload=data)

    pprint.pprint(r)
    print(r["StatusCode"])

    msg = base64.decodebytes(r["LogResult"].encode())
    print(msg.decode())

    return


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="fix missing thumbnails")
    parser.add_argument("profile", choices=["flowmsp-dev", "flowmsp-prod"])
    parser.add_argument("slug")
    parser.add_argument("location_id")
    parser.add_argument("image_id")
    parser.add_argument("function", choices=["createdAnnotatedImage", "createAnnotatedImage_new",
                                             "createAnnotatedThumbnail", "createAnnotatedThumbnail_new",
                                             "createAnnotatedImageAndThumbnail",
                                             "resize-image"])

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")

    client = MongoClient(mongo_uri)

    db = client.FlowMSP

    if args.slug not in get_customers(db):
        print("%s: unrecognized customer" % args.slug, file=sys.stderr)
        raise SystemExit(1)

    row = db["%s.Location" % args.slug].find_one({"_id": args.location_id})

    print(row["address"])
    
    for i, image in enumerate(row["images"]):
        if image["_id"] == args.image_id:
            call_function(args.profile,
                          args.slug,
                          args.location_id,
                          args.image_id,
                          image,
                          args.function)
